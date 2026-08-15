"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import { Card } from "@/components/Card";
import { VerdictBadge } from "@/components/VerdictBadge";
import { ScoreBar } from "@/components/ScoreBar";
import { fetchApi, Signal } from "@/lib/api";

export default function StockPage() {
  const params = useParams();
  const symbol = (params.symbol as string)?.toUpperCase();

  const [overview, setOverview] = useState<any>(null);
  const [insider, setInsider] = useState<any>(null);
  const [news, setNews] = useState<any>(null);
  const [analyst, setAnalyst] = useState<any>(null);
  const [earnings, setEarnings] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!symbol) return;
    Promise.all([
      fetchApi(`/api/stocks/${symbol}`),
      fetchApi(`/api/stocks/${symbol}/insider`),
      fetchApi(`/api/stocks/${symbol}/news`),
      fetchApi(`/api/stocks/${symbol}/analyst`),
      fetchApi(`/api/stocks/${symbol}/earnings`),
    ])
      .then(([ov, ins, nw, an, earn]) => {
        setOverview(ov);
        setInsider(ins);
        setNews(nw);
        setAnalyst(an);
        setEarnings(earn);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [symbol]);

  if (loading) {
    return <p className="text-slate-400">Loading {symbol}...</p>;
  }

  const price = overview?.price;
  const fundamentals = overview?.fundamentals;
  const signals: Signal[] = overview?.signals || [];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-baseline justify-between">
        <div>
          <h2 className="text-2xl font-bold text-white">{symbol}</h2>
          <p className="text-sm text-slate-400">
            {fundamentals?.sector || ""} {fundamentals?.industry ? `• ${fundamentals.industry}` : ""}
          </p>
        </div>
        {price && (
          <div className="text-right">
            <span className="text-2xl font-bold text-white">${price.price?.toFixed(2)}</span>
            <span className="text-sm text-slate-400 ml-2">{price.currency}</span>
          </div>
        )}
      </div>

      {/* AI Verdict */}
      {signals.length > 0 && (
        <Card title="AI Signals">
          <div className="space-y-2">
            {signals.map((sig, i) => (
              <div key={i} className="flex items-center justify-between py-2 border-b border-slate-700/30 last:border-0">
                <div>
                  <span className="text-sm font-medium text-slate-200">{sig.signal_type.replace(/_/g, " ")}</span>
                  <p className="text-xs text-slate-400 mt-0.5">{sig.description}</p>
                </div>
                <div className="w-24">
                  <ScoreBar score={sig.score} />
                </div>
              </div>
            ))}
          </div>
        </Card>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Fundamentals */}
        <Card title="Valuation">
          {fundamentals ? (
            <div className="grid grid-cols-2 gap-3 text-sm">
              <Stat label="PE Ratio" value={fundamentals.pe_ratio?.toFixed(1)} />
              <Stat label="EPS" value={`$${fundamentals.eps?.toFixed(2)}`} />
              <Stat label="Dividend Yield" value={`${(fundamentals.dividend_yield * 100)?.toFixed(2)}%`} />
              <Stat label="Debt/Equity" value={fundamentals.debt_to_equity?.toFixed(2)} />
              <Stat label="52W High" value={`$${fundamentals.week_52_high?.toFixed(2)}`} />
              <Stat label="52W Low" value={`$${fundamentals.week_52_low?.toFixed(2)}`} />
              <Stat label="Revenue TTM" value={formatLarge(fundamentals.revenue_ttm)} />
              <Stat label="Short Ratio" value={fundamentals.short_ratio?.toFixed(2)} />
            </div>
          ) : (
            <p className="text-slate-400 text-sm">No fundamentals data yet</p>
          )}
        </Card>

        {/* Analyst */}
        <Card title="Analyst Consensus">
          {analyst?.price_targets ? (
            <div className="space-y-3">
              <div className="grid grid-cols-3 gap-3 text-sm">
                <Stat label="Target Low" value={`$${analyst.price_targets.target_low?.toFixed(0)}`} />
                <Stat label="Target Median" value={`$${analyst.price_targets.target_median?.toFixed(0)}`} />
                <Stat label="Target High" value={`$${analyst.price_targets.target_high?.toFixed(0)}`} />
              </div>
              <div className="text-sm">
                <span className="text-slate-400">Upside: </span>
                <span className={`font-semibold ${analyst.price_targets.upside_pct > 0 ? "text-accent-green" : "text-accent-red"}`}>
                  {analyst.price_targets.upside_pct?.toFixed(0)}%
                </span>
                <span className="text-slate-400 ml-3">({analyst.price_targets.number_of_analysts} analysts)</span>
              </div>
            </div>
          ) : (
            <p className="text-slate-400 text-sm">No analyst data yet</p>
          )}
        </Card>

        {/* Insider Activity */}
        <Card title="Insider Activity (90 days)">
          {insider?.transactions?.length > 0 ? (
            <div className="space-y-2">
              <div className="flex items-center gap-2 mb-3">
                <span className={`text-sm font-semibold ${insider.net_value > 0 ? "text-accent-green" : "text-accent-red"}`}>
                  Net: ${formatLarge(Math.abs(insider.net_value))} ({insider.net_direction})
                </span>
              </div>
              {insider.transactions.slice(0, 5).map((tx: any, i: number) => (
                <div key={i} className="flex justify-between text-xs py-1 border-b border-slate-700/30">
                  <span className="text-slate-300">{tx.insider_name}</span>
                  <span className={tx.transaction_type === "P" ? "text-accent-green" : "text-accent-red"}>
                    {tx.transaction_type === "P" ? "BUY" : "SELL"} ${formatLarge(tx.total_value)}
                  </span>
                </div>
              ))}
            </div>
          ) : (
            <p className="text-slate-400 text-sm">No insider transactions</p>
          )}
        </Card>

        {/* Earnings History */}
        <Card title="Earnings History">
          {earnings?.history?.length > 0 ? (
            <div className="space-y-2">
              <div className="flex items-center gap-3 mb-3">
                <span className="text-sm text-slate-300">
                  Beat Rate: <span className="font-semibold text-white">{(earnings.beat_rate * 100).toFixed(0)}%</span>
                  ({earnings.beats}/{earnings.total_quarters})
                </span>
              </div>
              {earnings.history.slice(0, 6).map((q: any, i: number) => (
                <div key={i} className="flex justify-between text-xs py-1 border-b border-slate-700/30">
                  <span className="text-slate-400">{q.quarter_end}</span>
                  <span className="text-slate-300">EPS: ${q.eps_actual?.toFixed(2)} vs ${q.eps_estimate?.toFixed(2)}</span>
                  <span className={q.beat_eps ? "text-accent-green" : "text-accent-red"}>
                    {q.beat_eps ? "BEAT" : "MISS"} {q.eps_surprise_pct?.toFixed(1)}%
                  </span>
                </div>
              ))}
            </div>
          ) : (
            <p className="text-slate-400 text-sm">No earnings history</p>
          )}
        </Card>
      </div>

      {/* News */}
      <Card title={`News & Sentiment (avg: ${news?.avg_sentiment?.toFixed(2) || "—"})`}>
        {news?.articles?.length > 0 ? (
          <div className="space-y-2">
            {news.articles.slice(0, 8).map((article: any, i: number) => (
              <div key={i} className="flex items-start justify-between py-2 border-b border-slate-700/30 last:border-0">
                <div className="flex-1">
                  <a href={article.url} target="_blank" rel="noopener" className="text-sm text-slate-200 hover:text-white">
                    {article.headline}
                  </a>
                  <p className="text-xs text-slate-400 mt-0.5">{article.source} • {article.published_at?.slice(0, 10)}</p>
                </div>
                <span className={`text-xs font-medium ml-3 ${article.sentiment_score > 0 ? "text-accent-green" : article.sentiment_score < 0 ? "text-accent-red" : "text-slate-400"}`}>
                  {article.sentiment_score > 0 ? "+" : ""}{article.sentiment_score?.toFixed(2)}
                </span>
              </div>
            ))}
          </div>
        ) : (
          <p className="text-slate-400 text-sm">No recent news</p>
        )}
      </Card>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string | undefined }) {
  return (
    <div>
      <p className="text-xs text-slate-400">{label}</p>
      <p className="text-sm font-medium text-slate-200">{value || "—"}</p>
    </div>
  );
}

function formatLarge(n: number | undefined): string {
  if (!n) return "—";
  if (Math.abs(n) >= 1e12) return `$${(n / 1e12).toFixed(1)}T`;
  if (Math.abs(n) >= 1e9) return `$${(n / 1e9).toFixed(1)}B`;
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(1)}M`;
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(0)}K`;
  return `$${n.toFixed(0)}`;
}
