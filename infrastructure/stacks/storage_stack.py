"""
StorageStack — S3 data lake bucket, Glue Data Catalog, DynamoDB dedup table.
"""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import (
    aws_dynamodb as dynamodb,
    aws_glue_alpha as glue,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
)
from constructs import Construct


class StorageStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── S3 data lake bucket ───────────────────────────────────────────────
        self.data_bucket = s3.Bucket(
            self,
            "DataBucket",
            bucket_name=f"market-data-{self.account}-{self.region}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=False,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="IntelligentTiering",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=cdk.Duration.days(30),
                        )
                    ],
                )
            ],
        )

        # Seed initial config files into S3
        s3deploy.BucketDeployment(
            self,
            "SeedConfig",
            sources=[
                s3deploy.Source.asset(
                    "../config",
                    exclude=["tickers/*.csv"],  # tickers populated by setup script
                )
            ],
            destination_bucket=self.data_bucket,
            destination_key_prefix="config/",
        )

        # ── DynamoDB — news deduplication table ───────────────────────────────
        self.dedup_table = dynamodb.Table(
            self,
            "NewsDedupTable",
            table_name="market-news-dedup",
            partition_key=dynamodb.Attribute(
                name="article_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl",
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        # ── Glue Data Catalog ─────────────────────────────────────────────────
        self.glue_database_name = "market_data"

        glue_db = glue.Database(
            self,
            "GlueDatabase",
            database_name=self.glue_database_name,
        )

        # Stocks table
        glue.S3Table(
            self,
            "StocksTable",
            database=glue_db,
            table_name="stocks",
            bucket=self.data_bucket,
            s3_prefix="stocks/",
            data_format=glue.DataFormat.PARQUET,
            partition_keys=[
                glue.Column(name="date", type=glue.Schema.DATE),
                glue.Column(name="run_timestamp", type=glue.Schema.STRING),
                glue.Column(name="exchange", type=glue.Schema.STRING),
            ],
            columns=[
                glue.Column(name="symbol", type=glue.Schema.STRING),
                glue.Column(name="timestamp", type=glue.Schema.TIMESTAMP),
                glue.Column(name="price", type=glue.Schema.DOUBLE),
                glue.Column(name="open", type=glue.Schema.DOUBLE),
                glue.Column(name="high", type=glue.Schema.DOUBLE),
                glue.Column(name="low", type=glue.Schema.DOUBLE),
                glue.Column(name="close", type=glue.Schema.DOUBLE),
                glue.Column(name="volume", type=glue.Schema.BIG_INT),
                glue.Column(name="market_cap", type=glue.Schema.DOUBLE),
                glue.Column(name="currency", type=glue.Schema.STRING),
            ],
        )

        # Fundamentals table
        glue.S3Table(
            self,
            "FundamentalsTable",
            database=glue_db,
            table_name="fundamentals",
            bucket=self.data_bucket,
            s3_prefix="fundamentals/",
            data_format=glue.DataFormat.PARQUET,
            partition_keys=[
                glue.Column(name="date", type=glue.Schema.DATE),
                glue.Column(name="exchange", type=glue.Schema.STRING),
            ],
            columns=[
                glue.Column(name="symbol", type=glue.Schema.STRING),
                glue.Column(name="pe_ratio", type=glue.Schema.DOUBLE),
                glue.Column(name="pb_ratio", type=glue.Schema.DOUBLE),
                glue.Column(name="eps", type=glue.Schema.DOUBLE),
                glue.Column(name="dividend_yield", type=glue.Schema.DOUBLE),
                glue.Column(name="revenue_ttm", type=glue.Schema.DOUBLE),
                glue.Column(name="net_income_ttm", type=glue.Schema.DOUBLE),
                glue.Column(name="debt_to_equity", type=glue.Schema.DOUBLE),
                glue.Column(name="shares_outstanding", type=glue.Schema.BIG_INT),
                glue.Column(name="week_52_high", type=glue.Schema.DOUBLE),
                glue.Column(name="week_52_low", type=glue.Schema.DOUBLE),
                glue.Column(name="avg_volume_30d", type=glue.Schema.BIG_INT),
                glue.Column(name="short_ratio", type=glue.Schema.DOUBLE),
                glue.Column(name="short_percent_of_float", type=glue.Schema.DOUBLE),
            ],
        )

        # News table
        glue.S3Table(
            self,
            "NewsTable",
            database=glue_db,
            table_name="news",
            bucket=self.data_bucket,
            s3_prefix="news/",
            data_format=glue.DataFormat.PARQUET,
            partition_keys=[
                glue.Column(name="date", type=glue.Schema.DATE),
                glue.Column(name="run_timestamp", type=glue.Schema.STRING),
                glue.Column(name="source", type=glue.Schema.STRING),
            ],
            columns=[
                glue.Column(name="article_id", type=glue.Schema.STRING),
                glue.Column(name="headline", type=glue.Schema.STRING),
                glue.Column(name="summary", type=glue.Schema.STRING),
                glue.Column(name="url", type=glue.Schema.STRING),
                glue.Column(name="published_at", type=glue.Schema.TIMESTAMP),
                glue.Column(
                    name="symbols_mentioned",
                    type=glue.Schema.array(
                        input_string="array<string>", is_primitive=False
                    ),
                ),
                glue.Column(name="sentiment_score", type=glue.Schema.FLOAT),
                glue.Column(
                    name="topics",
                    type=glue.Schema.array(
                        input_string="array<string>", is_primitive=False
                    ),
                ),
                glue.Column(name="collection_timestamp", type=glue.Schema.TIMESTAMP),
            ],
        )

        # Splits table
        glue.S3Table(
            self,
            "SplitsTable",
            database=glue_db,
            table_name="splits",
            bucket=self.data_bucket,
            s3_prefix="corporate_actions/type=split/",
            data_format=glue.DataFormat.PARQUET,
            partition_keys=[
                glue.Column(name="date", type=glue.Schema.DATE),
                glue.Column(name="exchange", type=glue.Schema.STRING),
            ],
            columns=[
                glue.Column(name="symbol", type=glue.Schema.STRING),
                glue.Column(name="event_date", type=glue.Schema.DATE),
                glue.Column(name="ratio", type=glue.Schema.DOUBLE),
                glue.Column(name="collection_date", type=glue.Schema.DATE),
            ],
        )

        # Dividends table
        glue.S3Table(
            self,
            "DividendsTable",
            database=glue_db,
            table_name="dividends",
            bucket=self.data_bucket,
            s3_prefix="corporate_actions/type=dividend/",
            data_format=glue.DataFormat.PARQUET,
            partition_keys=[
                glue.Column(name="date", type=glue.Schema.DATE),
                glue.Column(name="exchange", type=glue.Schema.STRING),
            ],
            columns=[
                glue.Column(name="symbol", type=glue.Schema.STRING),
                glue.Column(name="ex_date", type=glue.Schema.DATE),
                glue.Column(name="amount", type=glue.Schema.DOUBLE),
                glue.Column(name="collection_date", type=glue.Schema.DATE),
            ],
        )

        # Analyst — price targets table
        glue.S3Table(
            self,
            "AnalystPriceTargetsTable",
            database=glue_db,
            table_name="analyst_price_targets",
            bucket=self.data_bucket,
            s3_prefix="analyst/type=price_targets/",
            data_format=glue.DataFormat.PARQUET,
            partition_keys=[
                glue.Column(name="date", type=glue.Schema.DATE),
                glue.Column(name="exchange", type=glue.Schema.STRING),
            ],
            columns=[
                glue.Column(name="symbol", type=glue.Schema.STRING),
                glue.Column(name="current_price", type=glue.Schema.DOUBLE),
                glue.Column(name="target_low", type=glue.Schema.DOUBLE),
                glue.Column(name="target_mean", type=glue.Schema.DOUBLE),
                glue.Column(name="target_high", type=glue.Schema.DOUBLE),
                glue.Column(name="target_median", type=glue.Schema.DOUBLE),
                glue.Column(name="number_of_analysts", type=glue.Schema.INTEGER),
                glue.Column(name="upside_pct", type=glue.Schema.DOUBLE),
            ],
        )

        # Analyst — recommendations table
        glue.S3Table(
            self,
            "AnalystRecommendationsTable",
            database=glue_db,
            table_name="analyst_recommendations",
            bucket=self.data_bucket,
            s3_prefix="analyst/type=recommendations/",
            data_format=glue.DataFormat.PARQUET,
            partition_keys=[
                glue.Column(name="date", type=glue.Schema.DATE),
                glue.Column(name="exchange", type=glue.Schema.STRING),
            ],
            columns=[
                glue.Column(name="symbol", type=glue.Schema.STRING),
                glue.Column(name="period", type=glue.Schema.STRING),
                glue.Column(name="strong_buy", type=glue.Schema.INTEGER),
                glue.Column(name="buy", type=glue.Schema.INTEGER),
                glue.Column(name="hold", type=glue.Schema.INTEGER),
                glue.Column(name="sell", type=glue.Schema.INTEGER),
                glue.Column(name="strong_sell", type=glue.Schema.INTEGER),
                glue.Column(name="consensus", type=glue.Schema.STRING),
            ],
        )

        # Analyst — earnings dates table
        glue.S3Table(
            self,
            "EarningsDatesTable",
            database=glue_db,
            table_name="earnings_dates",
            bucket=self.data_bucket,
            s3_prefix="analyst/type=earnings_dates/",
            data_format=glue.DataFormat.PARQUET,
            partition_keys=[
                glue.Column(name="date", type=glue.Schema.DATE),
                glue.Column(name="exchange", type=glue.Schema.STRING),
            ],
            columns=[
                glue.Column(name="symbol", type=glue.Schema.STRING),
                glue.Column(name="collection_date", type=glue.Schema.DATE),
                glue.Column(name="earnings_date", type=glue.Schema.TIMESTAMP),
                glue.Column(name="eps_estimate", type=glue.Schema.DOUBLE),
                glue.Column(name="reported_eps", type=glue.Schema.DOUBLE),
                glue.Column(name="surprise_pct", type=glue.Schema.DOUBLE),
                glue.Column(name="is_future", type=glue.Schema.BOOLEAN),
            ],
        )

        # Financial statements — single long-format table covering
        # income statement, balance sheet, and cash flow (quarterly + annual)
        glue.S3Table(
            self,
            "FinancialsTable",
            database=glue_db,
            table_name="financials",
            bucket=self.data_bucket,
            s3_prefix="financials/",
            data_format=glue.DataFormat.PARQUET,
            partition_keys=[
                glue.Column(name="date", type=glue.Schema.DATE),
                glue.Column(name="exchange", type=glue.Schema.STRING),
            ],
            columns=[
                glue.Column(name="symbol", type=glue.Schema.STRING),
                glue.Column(name="collection_date", type=glue.Schema.DATE),
                glue.Column(name="statement_type", type=glue.Schema.STRING),
                glue.Column(name="period_type", type=glue.Schema.STRING),
                glue.Column(name="period_end_date", type=glue.Schema.DATE),
                glue.Column(name="line_item", type=glue.Schema.STRING),
                glue.Column(name="value", type=glue.Schema.DOUBLE),
            ],
        )

        # Insider transactions table
        glue.S3Table(
            self,
            "InsiderTransactionsTable",
            database=glue_db,
            table_name="insider_transactions",
            bucket=self.data_bucket,
            s3_prefix="insider_transactions/",
            data_format=glue.DataFormat.PARQUET,
            partition_keys=[
                glue.Column(name="date", type=glue.Schema.DATE),
                glue.Column(name="exchange", type=glue.Schema.STRING),
            ],
            columns=[
                glue.Column(name="transaction_id", type=glue.Schema.STRING),
                glue.Column(name="symbol", type=glue.Schema.STRING),
                glue.Column(name="collection_date", type=glue.Schema.DATE),
                glue.Column(name="filing_date", type=glue.Schema.DATE),
                glue.Column(name="transaction_date", type=glue.Schema.DATE),
                glue.Column(name="insider_name", type=glue.Schema.STRING),
                glue.Column(name="insider_title", type=glue.Schema.STRING),
                glue.Column(name="transaction_type", type=glue.Schema.STRING),
                glue.Column(name="is_derivative", type=glue.Schema.BOOLEAN),
                glue.Column(name="shares", type=glue.Schema.DOUBLE),
                glue.Column(name="price_per_share", type=glue.Schema.DOUBLE),
                glue.Column(name="total_value", type=glue.Schema.DOUBLE),
                glue.Column(name="shares_owned_after", type=glue.Schema.DOUBLE),
                glue.Column(name="ownership_type", type=glue.Schema.STRING),
                glue.Column(name="source", type=glue.Schema.STRING),
            ],
        )

        # Signals table
        glue.S3Table(
            self,
            "SignalsTable",
            database=glue_db,
            table_name="signals",
            bucket=self.data_bucket,
            s3_prefix="signals/",
            data_format=glue.DataFormat.PARQUET,
            partition_keys=[
                glue.Column(name="date", type=glue.Schema.DATE),
            ],
            columns=[
                glue.Column(name="symbol", type=glue.Schema.STRING),
                glue.Column(name="exchange", type=glue.Schema.STRING),
                glue.Column(name="signal_date", type=glue.Schema.DATE),
                glue.Column(name="signal_type", type=glue.Schema.STRING),
                glue.Column(name="score", type=glue.Schema.DOUBLE),
                glue.Column(name="confidence", type=glue.Schema.DOUBLE),
                glue.Column(name="description", type=glue.Schema.STRING),
                glue.Column(name="evidence", type=glue.Schema.STRING),
            ],
        )

        # Opportunities table (composite scored stocks)
        glue.S3Table(
            self,
            "OpportunitiesTable",
            database=glue_db,
            table_name="opportunities",
            bucket=self.data_bucket,
            s3_prefix="signals/type=opportunities/",
            data_format=glue.DataFormat.PARQUET,
            partition_keys=[
                glue.Column(name="date", type=glue.Schema.DATE),
            ],
            columns=[
                glue.Column(name="symbol", type=glue.Schema.STRING),
                glue.Column(name="exchange", type=glue.Schema.STRING),
                glue.Column(name="signal_date", type=glue.Schema.DATE),
                glue.Column(name="composite_score", type=glue.Schema.DOUBLE),
                glue.Column(name="verdict", type=glue.Schema.STRING),
                glue.Column(name="signals_fired", type=glue.Schema.INTEGER),
                glue.Column(name="top_signals", type=glue.Schema.STRING),
                glue.Column(name="sector", type=glue.Schema.STRING),
                glue.Column(name="current_price", type=glue.Schema.DOUBLE),
                glue.Column(name="pe_ratio", type=glue.Schema.DOUBLE),
                glue.Column(name="earnings_date", type=glue.Schema.STRING),
                glue.Column(name="beat_rate", type=glue.Schema.DOUBLE),
                glue.Column(name="insider_net_30d", type=glue.Schema.DOUBLE),
                glue.Column(name="sentiment_score", type=glue.Schema.DOUBLE),
                glue.Column(name="analyst_upside_pct", type=glue.Schema.DOUBLE),
            ],
        )

        # ── Outputs ───────────────────────────────────────────────────────────
        cdk.CfnOutput(self, "DataBucketName", value=self.data_bucket.bucket_name)
        cdk.CfnOutput(self, "DedupTableName", value=self.dedup_table.table_name)
        cdk.CfnOutput(self, "GlueDatabaseName", value=self.glue_database_name)
