"""
Lambda: collect-news

Polls multiple free financial news sources every 30 minutes, deduplicates
articles, scores sentiment, and writes Parquet to S3.

Sources:
  - RSS feeds (Reuters, MarketWatch, CNBC, FT, SEC EDGAR)
  - Marketaux REST API (100 req/day free)
  - GDELT 2.0 Doc API (unlimited, public)

Input:
    { "run_timestamp": "2026-07-24T14:30:00Z", "date": "2026-07-24" }

Output:
    { "articles_new": 87, "articles_skipped_duplicate": 34, "news_s3_keys": [...] }
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

import boto3
import feedparser
import pandas as pd
import pyarrow as pa
import requests
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.typing import LambdaContext

from parquet_writer import write_parquet

logger = Logger()
metrics = Metrics(namespace="MarketData")
dynamodb = boto3.resource("dynamodb")
secretsmanager = boto3.client("secretsmanager")

DEDUP_TABLE = os.environ["DEDUP_TABLE"]
MARKETAUX_SECRET = os.environ["MARKETAUX_SECRET"]
DATA_BUCKET = os.environ["DATA_BUCKET"]

vader = SentimentIntensityAnalyzer()

# ── Parquet schema ─────────────────────────────────────────────────────────────

NEWS_SCHEMA = pa.schema(
    [
        pa.field("article_id", pa.string()),
        pa.field("source", pa.string()),
        pa.field("headline", pa.string()),
        pa.field("summary", pa.string()),
        pa.field("url", pa.string()),
        pa.field("published_at", pa.timestamp("us", tz="UTC")),
        pa.field("symbols_mentioned", pa.list_(pa.string())),
        pa.field("sentiment_score", pa.float32()),
        pa.field("topics", pa.list_(pa.string())),
        pa.field("collection_timestamp", pa.timestamp("us", tz="UTC")),
    ]
)

# ── Topic keyword map ─────────────────────────────────────────────────────────

TOPIC_KEYWORDS: dict[str, list[str]] = {
    "earnings": ["earnings", "revenue", "profit", "loss", "eps", "quarterly results"],
    "merger": ["merger", "acquisition", "takeover", "buyout", "m&a", "deal"],
    "ipo": ["ipo", "initial public offering", "listing", "float"],
    "central_bank": [
        "federal reserve",
        "fed",
        "ecb",
        "boe",
        "interest rate",
        "rate hike",
        "rate cut",
    ],
    "macro": ["gdp", "inflation", "unemployment", "recession", "economic growth"],
    "dividend": ["dividend", "payout", "yield"],
    "analyst": ["upgrade", "downgrade", "price target", "buy rating", "sell rating"],
    "investment": ["investment", "venture capital", "private equity", "fund", "stake"],
    "regulation": [
        "sec",
        "fca",
        "regulation",
        "fine",
        "penalty",
        "lawsuit",
        "antitrust",
    ],
    "geopolitical": ["sanctions", "tariff", "trade war", "conflict", "election"],
}


def _detect_topics(text: str) -> list[str]:
    text_lower = text.lower()
    return [
        topic
        for topic, keywords in TOPIC_KEYWORDS.items()
        if any(kw in text_lower for kw in keywords)
    ]


# ── Ticker extraction ─────────────────────────────────────────────────────────

_TICKER_RE = re.compile(r"\b([A-Z]{2,5})\b")
_COMMON_WORDS = {
    "THE",
    "AND",
    "FOR",
    "WITH",
    "FROM",
    "THAT",
    "THIS",
    "WILL",
    "HAVE",
    "MORE",
    "INTO",
    "THAN",
    "THEY",
    "BEEN",
    "WERE",
    "SAID",
    "OVER",
    "AFTER",
    "ALSO",
    "WHICH",
    "WHEN",
    "YEAR",
    "SAYS",
    "STOCK",
    "CORP",
    "INC",
    "LTD",
    "CEO",
    "CFO",
    "IPO",
    "ETF",
    "GDP",
    "ECB",
    "FED",
    "SEC",
    "FCA",
    "BOE",
    "NASDAQ",
    "NYSE",
    "LSE",
    "USD",
    "EUR",
    "GBP",
    "JPY",
}


def _extract_tickers(text: str) -> list[str]:
    candidates = _TICKER_RE.findall(text)
    return list({t for t in candidates if t not in _COMMON_WORDS})


# ── Article ID ────────────────────────────────────────────────────────────────


def _article_id(url: str) -> str:
    return hashlib.sha256(url.strip().lower().encode()).hexdigest()


# ── Dedup ────────────────────────────────────────────────────────────────────


def _is_duplicate(table, article_id: str) -> bool:
    resp = table.get_item(Key={"article_id": article_id})
    return "Item" in resp


def _mark_seen(table, article_id: str, source: str, collected_at: str) -> None:
    ttl = int((datetime.now(timezone.utc) + timedelta(hours=48)).timestamp())
    table.put_item(
        Item={
            "article_id": article_id,
            "ttl": ttl,
            "source": source,
            "collected_at": collected_at,
        }
    )


# ── Sentiment ─────────────────────────────────────────────────────────────────


def _sentiment(headline: str, summary: str) -> float:
    text = f"{headline} {summary}"
    scores = vader.polarity_scores(text)
    return round(scores["compound"], 4)


# ── Parse datetime safely ─────────────────────────────────────────────────────


def _parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            pass
    return datetime.now(timezone.utc)


# ── RSS Feed Collection ───────────────────────────────────────────────────────

RSS_FEEDS: dict[str, str] = {
    "reuters": "https://feeds.reuters.com/reuters/businessNews",
    "marketwatch": "https://feeds.marketwatch.com/marketwatch/topstories",
    "cnbc": "https://www.cnbc.com/id/10000664/device/rss/rss.html",
    "ft": "https://www.ft.com/rss/home/uk",
    "sec_8k": (
        "https://www.sec.gov/cgi-bin/browse-edgar"
        "?action=getcurrent&type=8-K&dateb=&owner=include&count=40&output=atom"
    ),
    # Google News — business/finance
    "google_business": "https://news.google.com/rss/topics/CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx6TVdZU0FtVnVHZ0pWVXlnQVAB",
    "google_markets": "https://news.google.com/rss/topics/CAAqKAgKIiJDQkFTRXdvSkwyMHZNR3gxZEhGZkVnSmxiaG9DVlZNb0FBUAE",
    # Seeking Alpha
    "seekingalpha_market": "https://seekingalpha.com/market_currents.xml",
    "seekingalpha_news": "https://seekingalpha.com/news.xml",
    # PRNewswire — corporate press releases (earnings, M&A, dividends)
    "prnewswire_finance": "https://www.prnewswire.com/rss/financial-services-latest-news/financial-services-latest-news-list.rss",
    "prnewswire_earnings": "https://www.prnewswire.com/rss/earnings-latest-news/earnings-latest-news-list.rss",
    # BusinessWire — corporate announcements
    "businesswire": "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeEFpRWQ==",
    # Investing.com RSS
    "investing_news": "https://www.investing.com/rss/news.rss",
    "investing_stock": "https://www.investing.com/rss/news_14.rss",
}


def collect_rss(
    source: str, feed_url: str, table, collection_ts: datetime
) -> tuple[list[dict], int]:
    articles: list[dict] = []
    skipped = 0
    try:
        feed = feedparser.parse(feed_url)
        for entry in feed.entries:
            url = entry.get("link", "")
            if not url:
                continue
            aid = _article_id(url)
            if _is_duplicate(table, aid):
                skipped += 1
                continue
            headline = entry.get("title", "")
            summary = entry.get("summary", entry.get("description", ""))
            published_at = _parse_dt(
                entry.get("published") or entry.get("updated") or ""
            )
            text = f"{headline} {summary}"
            articles.append(
                {
                    "article_id": aid,
                    "source": source,
                    "headline": headline,
                    "summary": summary[:1000],
                    "url": url,
                    "published_at": published_at,
                    "symbols_mentioned": _extract_tickers(text),
                    "sentiment_score": _sentiment(headline, summary),
                    "topics": _detect_topics(text),
                    "collection_timestamp": collection_ts,
                }
            )
            _mark_seen(table, aid, source, collection_ts.isoformat())
    except Exception as exc:
        logger.warning(
            "RSS feed error",
            extra={"source": source, "url": feed_url, "error": str(exc)},
        )
    return articles, skipped


# ── Marketaux Collection ──────────────────────────────────────────────────────


def _get_marketaux_key() -> str:
    secret = secretsmanager.get_secret_value(SecretId=MARKETAUX_SECRET)
    return json.loads(secret["SecretString"])["api_key"]


def collect_marketaux(table, collection_ts: datetime) -> tuple[list[dict], int]:
    articles: list[dict] = []
    skipped = 0
    try:
        api_key = _get_marketaux_key()
        resp = requests.get(
            "https://api.marketaux.com/v1/news/all",
            params={
                "api_token": api_key,
                "filter_entities": "true",
                "language": "en",
                "limit": 50,
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        for item in data.get("data", []):
            url = item.get("url", "")
            if not url:
                continue
            aid = _article_id(url)
            if _is_duplicate(table, aid):
                skipped += 1
                continue
            headline = item.get("title", "")
            summary = item.get("description", "")
            published_at = _parse_dt(item.get("published_at", ""))
            symbols = [
                e.get("symbol", "")
                for e in item.get("entities", [])
                if e.get("type") == "equity"
            ]
            text = f"{headline} {summary}"
            articles.append(
                {
                    "article_id": aid,
                    "source": "marketaux",
                    "headline": headline,
                    "summary": summary[:1000],
                    "url": url,
                    "published_at": published_at,
                    "symbols_mentioned": [s for s in symbols if s],
                    "sentiment_score": _sentiment(headline, summary),
                    "topics": _detect_topics(text),
                    "collection_timestamp": collection_ts,
                }
            )
            _mark_seen(table, aid, "marketaux", collection_ts.isoformat())
    except Exception as exc:
        logger.warning("Marketaux error", extra={"error": str(exc)})
    return articles, skipped


# ── GDELT Doc API Collection ──────────────────────────────────────────────────


def collect_gdelt(table, collection_ts: datetime) -> tuple[list[dict], int]:
    articles: list[dict] = []
    skipped = 0
    try:
        resp = requests.get(
            "https://api.gdeltproject.org/api/v2/doc/doc",
            params={
                "mode": "artlist",
                "theme": "ECON_STOCKMARKET",
                "maxrecords": 75,
                "timespan": "30min",
                "format": "json",
            },
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        for item in data.get("articles", []):
            url = item.get("url", "")
            if not url:
                continue
            aid = _article_id(url)
            if _is_duplicate(table, aid):
                skipped += 1
                continue
            headline = item.get("title", "")
            summary = ""
            published_at = _parse_dt(item.get("seendate", ""))
            text = headline
            articles.append(
                {
                    "article_id": aid,
                    "source": "gdelt",
                    "headline": headline,
                    "summary": summary,
                    "url": url,
                    "published_at": published_at,
                    "symbols_mentioned": _extract_tickers(text),
                    "sentiment_score": _sentiment(headline, summary),
                    "topics": _detect_topics(text),
                    "collection_timestamp": collection_ts,
                }
            )
            _mark_seen(table, aid, "gdelt", collection_ts.isoformat())
    except Exception as exc:
        logger.warning("GDELT error", extra={"error": str(exc)})
    return articles, skipped


# ── Reddit JSON Collection ────────────────────────────────────────────────────

REDDIT_FEEDS: dict[str, str] = {
    "reddit_stocks": "https://old.reddit.com/r/stocks/.rss",
    "reddit_wsb": "https://old.reddit.com/r/wallstreetbets/.rss",
    "reddit_investing": "https://old.reddit.com/r/investing/.rss",
}


def collect_reddit(table, collection_ts: datetime) -> tuple[list[dict], int]:
    """Collect stock-related posts from finance subreddits via RSS."""
    articles: list[dict] = []
    skipped = 0

    for source, url in REDDIT_FEEDS.items():
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                post_url = entry.get("link", "")
                if not post_url:
                    continue
                aid = _article_id(post_url)
                if _is_duplicate(table, aid):
                    skipped += 1
                    continue
                headline = entry.get("title", "")
                summary = (entry.get("summary", "") or "")[:1000]
                published_at = _parse_dt(
                    entry.get("published") or entry.get("updated") or ""
                )
                text = f"{headline} {summary}"
                articles.append(
                    {
                        "article_id": aid,
                        "source": source,
                        "headline": headline,
                        "summary": summary,
                        "url": post_url,
                        "published_at": published_at,
                        "symbols_mentioned": _extract_tickers(text),
                        "sentiment_score": _sentiment(headline, summary),
                        "topics": _detect_topics(text),
                        "collection_timestamp": collection_ts,
                    }
                )
                _mark_seen(table, aid, source, collection_ts.isoformat())
        except Exception as exc:
            logger.warning("Reddit error", extra={"source": source, "error": str(exc)})
    return articles, skipped


# ── Yahoo Finance per-ticker news ─────────────────────────────────────────────

# Top 50 most-watched tickers — we pull news for these specifically
# (pulling for all 4000+ would exceed Lambda timeout)
YF_NEWS_TICKERS = [
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "META",
    "GOOGL",
    "TSLA",
    "AVGO",
    "JPM",
    "V",
    "UNH",
    "XOM",
    "WMT",
    "MA",
    "JNJ",
    "HD",
    "PG",
    "CVX",
    "MRK",
    "ABBV",
    "LLY",
    "BAC",
    "KO",
    "PEP",
    "NFLX",
    "AMD",
    "CRM",
    "COST",
    "ADBE",
    "TMO",
    "DIS",
    "INTC",
    "BA",
    "GS",
    "CAT",
    "NKE",
    "QCOM",
    "SBUX",
    "GE",
    "BLK",
    "AMGN",
    "PYPL",
    "BKNG",
    "TXN",
    "COIN",
    "PLTR",
    "SOFI",
    "RIVN",
    "ARM",
    "SMCI",
]


def collect_yfinance_news(table, collection_ts: datetime) -> tuple[list[dict], int]:
    """Pull news for top tickers via yfinance Ticker.news property."""
    import yfinance as yf

    articles: list[dict] = []
    skipped = 0

    for symbol in YF_NEWS_TICKERS:
        try:
            ticker = yf.Ticker(symbol)
            news_items = ticker.news or []
            for item in news_items:
                # yfinance 1.5.x returns nested structure: item['content']
                content = item.get("content", item)
                url = ""
                if isinstance(content, dict):
                    canon = content.get("canonicalUrl", {})
                    url = canon.get("url", "") if isinstance(canon, dict) else ""
                    if not url:
                        click = content.get("clickThroughUrl", {})
                        url = click.get("url", "") if isinstance(click, dict) else ""
                if not url:
                    url = item.get("link", item.get("url", ""))
                if not url:
                    continue
                aid = _article_id(url)
                if _is_duplicate(table, aid):
                    skipped += 1
                    continue
                headline = (
                    content.get("title", "")
                    if isinstance(content, dict)
                    else item.get("title", "")
                )
                summary = (
                    content.get("summary", "")
                    if isinstance(content, dict)
                    else item.get("summary", "")
                )[:1000]
                pub_raw = (
                    content.get("pubDate", content.get("displayTime", ""))
                    if isinstance(content, dict)
                    else item.get("providerPublishTime", 0)
                )
                if isinstance(pub_raw, (int, float)) and pub_raw > 0:
                    published_at = datetime.fromtimestamp(pub_raw, tz=timezone.utc)
                else:
                    published_at = _parse_dt(pub_raw)
                provider = (
                    content.get("provider", {}) if isinstance(content, dict) else {}
                )
                publisher = (
                    provider.get("displayName", "yahoo_finance")
                    if isinstance(provider, dict)
                    else "yahoo_finance"
                )
                text = f"{headline} {summary}"
                articles.append(
                    {
                        "article_id": aid,
                        "source": f"yfinance_{publisher}".lower().replace(" ", "_"),
                        "headline": headline,
                        "summary": summary,
                        "url": url,
                        "published_at": published_at,
                        "symbols_mentioned": list(
                            set([symbol] + _extract_tickers(text))
                        ),
                        "sentiment_score": _sentiment(headline, summary),
                        "topics": _detect_topics(text),
                        "collection_timestamp": collection_ts,
                    }
                )
                _mark_seen(table, aid, "yfinance", collection_ts.isoformat())
        except Exception:
            pass  # skip individual ticker failures silently
        time.sleep(0.2)  # pace to avoid rate limits

    return articles, skipped


# ── Handler ────────────────────────────────────────────────────────────────────


@logger.inject_lambda_context(log_event=True)
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    run_timestamp: str = event["run_timestamp"]
    date_str: str = run_timestamp[:10]
    run_ts_safe = run_timestamp.replace(":", "-").rstrip("Z") + "Z"
    collection_ts = datetime.now(timezone.utc)

    table = dynamodb.Table(DEDUP_TABLE)

    all_articles: list[dict] = []
    total_skipped = 0

    # RSS feeds
    for source, url in RSS_FEEDS.items():
        arts, skipped = collect_rss(source, url, table, collection_ts)
        all_articles.extend(arts)
        total_skipped += skipped
        logger.info(
            "RSS collected",
            extra={"source": source, "new": len(arts), "skipped": skipped},
        )

    # Marketaux
    arts, skipped = collect_marketaux(table, collection_ts)
    all_articles.extend(arts)
    total_skipped += skipped

    # GDELT
    arts, skipped = collect_gdelt(table, collection_ts)
    all_articles.extend(arts)
    total_skipped += skipped

    # Reddit
    arts, skipped = collect_reddit(table, collection_ts)
    all_articles.extend(arts)
    total_skipped += skipped
    logger.info("Reddit collected", extra={"new": len(arts), "skipped": skipped})

    # Yahoo Finance per-ticker news (top 50 stocks)
    arts, skipped = collect_yfinance_news(table, collection_ts)
    all_articles.extend(arts)
    total_skipped += skipped
    logger.info("YFinance news collected", extra={"new": len(arts), "skipped": skipped})

    news_s3_keys: list[str] = []

    if all_articles:
        # Group by source for partitioned write
        df = pd.DataFrame(all_articles)
        for source_name, group_df in df.groupby("source"):
            s3_key = (
                f"news/date={date_str}"
                f"/run_timestamp={run_ts_safe}/source={source_name}/data.parquet"
            )
            write_parquet(group_df.reset_index(drop=True), s3_key, schema=NEWS_SCHEMA)
            news_s3_keys.append(s3_key)
            logger.info(
                "News written to S3",
                extra={"source": source_name, "key": s3_key, "rows": len(group_df)},
            )

    metrics.add_metric(
        name="NewsArticlesCollected",
        unit=MetricUnit.Count,
        value=len(all_articles),
    )

    return {
        "articles_new": len(all_articles),
        "articles_skipped_duplicate": total_skipped,
        "news_s3_keys": news_s3_keys,
    }
