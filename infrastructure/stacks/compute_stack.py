"""
ComputeStack — Lambda Layer + all five Lambda functions + ECS Fargate fundamentals task.
"""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import (
    aws_dynamodb as dynamodb,
    aws_ec2 as ec2,
    aws_ecr_assets as ecr_assets,
    aws_ecs as ecs,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
)
from constructs import Construct

PYTHON_RUNTIME = lambda_.Runtime.PYTHON_3_12
LAYER_PATH = "../layer"
LAMBDAS_PATH = "../lambdas"


class ComputeStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        data_bucket: s3.Bucket,
        dedup_table: dynamodb.Table,
        glue_database_name: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self._bucket = data_bucket
        self._dedup = dedup_table
        self._glue_db = glue_database_name

        # ── Secrets (pre-created manually; CDK just references them) ─────────
        marketaux_secret = secretsmanager.Secret.from_secret_name_v2(
            self, "MarketauxSecret", "market-data/marketaux-api-key"
        )

        # ── AWS SDK for pandas managed layer (pandas + pyarrow + numpy) ──────
        aws_sdk_pandas_layer = lambda_.LayerVersion.from_layer_version_arn(
            self,
            "AWSSDKPandasLayer",
            layer_version_arn=(
                "arn:aws:lambda:us-east-1:336392948345"
                ":layer:AWSSDKPandas-Python312:24"
            ),
        )

        # ── Custom layer — lightweight deps only (no pandas/numpy/pyarrow) ───
        custom_layer = lambda_.LayerVersion(
            self,
            "MarketDataCustomLayer",
            layer_version_name="market-data-custom-deps",
            code=lambda_.Code.from_asset(LAYER_PATH),
            compatible_runtimes=[PYTHON_RUNTIME],
            description="yfinance, vaderSentiment, feedparser, requests, aws-lambda-powertools",
        )

        # Both layers attached to every function
        all_layers = [aws_sdk_pandas_layer, custom_layer]

        common_env = {
            "DATA_BUCKET": data_bucket.bucket_name,
            "GLUE_DATABASE": glue_database_name,
            "POWERTOOLS_SERVICE_NAME": "market-data-pipeline",
            "LOG_LEVEL": "INFO",
        }

        # ── check-market-hours ────────────────────────────────────────────────
        self.check_market_hours_fn = lambda_.Function(
            self,
            "CheckMarketHours",
            function_name="market-data-check-market-hours",
            runtime=PYTHON_RUNTIME,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset(f"{LAMBDAS_PATH}/check_market_hours"),
            layers=all_layers,
            memory_size=256,
            timeout=cdk.Duration.minutes(15),
            environment={**common_env},
        )
        data_bucket.grant_read(self.check_market_hours_fn, "config/*")

        # ── collect-stocks ────────────────────────────────────────────────────
        self.collect_stocks_fn = lambda_.Function(
            self,
            "CollectStocks",
            function_name="market-data-collect-stocks",
            runtime=PYTHON_RUNTIME,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset(f"{LAMBDAS_PATH}/collect_stocks"),
            layers=all_layers,
            memory_size=768,
            timeout=cdk.Duration.minutes(15),
            environment={**common_env},
        )
        data_bucket.grant_read(self.collect_stocks_fn, "config/*")
        data_bucket.grant_read_write(self.collect_stocks_fn, "stocks/*")
        self.collect_stocks_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
            )
        )

        # ── compact-stocks ────────────────────────────────────────────────────
        self.compact_stocks_fn = lambda_.Function(
            self,
            "CompactStocks",
            function_name="market-data-compact-stocks",
            runtime=PYTHON_RUNTIME,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset(f"{LAMBDAS_PATH}/compact_stocks"),
            layers=all_layers,
            memory_size=256,
            timeout=cdk.Duration.minutes(15),
            environment={**common_env},
        )
        data_bucket.grant_read_write(self.compact_stocks_fn, "stocks/*")

        # ── collect-news ──────────────────────────────────────────────────────
        self.collect_news_fn = lambda_.Function(
            self,
            "CollectNews",
            function_name="market-data-collect-news",
            runtime=PYTHON_RUNTIME,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset(f"{LAMBDAS_PATH}/collect_news"),
            layers=all_layers,
            memory_size=320,
            timeout=cdk.Duration.minutes(15),
            environment={
                **common_env,
                "DEDUP_TABLE": dedup_table.table_name,
                "MARKETAUX_SECRET": marketaux_secret.secret_name,
            },
        )
        data_bucket.grant_write(self.collect_news_fn, "news/*")
        dedup_table.grant_read_write_data(self.collect_news_fn)
        marketaux_secret.grant_read(self.collect_news_fn)

        # ── update-glue-catalog ───────────────────────────────────────────────
        self.update_glue_catalog_fn = lambda_.Function(
            self,
            "UpdateGlueCatalog",
            function_name="market-data-update-glue-catalog",
            runtime=PYTHON_RUNTIME,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset(f"{LAMBDAS_PATH}/update_glue_catalog"),
            layers=all_layers,
            memory_size=256,
            timeout=cdk.Duration.minutes(15),
            environment={**common_env},
        )
        data_bucket.grant_read(self.update_glue_catalog_fn)
        self.update_glue_catalog_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetTable",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:BatchCreatePartition",
                    "glue:CreatePartition",
                ],
                resources=[
                    f"arn:aws:glue:{self.region}:{self.account}:catalog",
                    f"arn:aws:glue:{self.region}:{self.account}:database/{glue_database_name}",
                    f"arn:aws:glue:{self.region}:{self.account}:table/{glue_database_name}/*",
                ],
            )
        )

        # ── collect-corporate-actions ─────────────────────────────────────────
        self.collect_corporate_actions_fn = lambda_.Function(
            self,
            "CollectCorporateActions",
            function_name="market-data-collect-corporate-actions",
            runtime=PYTHON_RUNTIME,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset(f"{LAMBDAS_PATH}/collect_corporate_actions"),
            layers=all_layers,
            memory_size=512,
            timeout=cdk.Duration.minutes(15),
            environment={**common_env},
        )
        data_bucket.grant_read(self.collect_corporate_actions_fn, "config/*")
        data_bucket.grant_write(
            self.collect_corporate_actions_fn, "corporate_actions/*"
        )
        self.collect_corporate_actions_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
            )
        )

        # ── collect-insider-transactions ──────────────────────────────────────
        self.collect_insider_transactions_fn = lambda_.Function(
            self,
            "CollectInsiderTransactions",
            function_name="market-data-collect-insider-transactions",
            runtime=PYTHON_RUNTIME,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset(
                f"{LAMBDAS_PATH}/collect_insider_transactions"
            ),
            layers=all_layers,
            memory_size=512,
            timeout=cdk.Duration.minutes(15),
            environment={**common_env},
        )
        data_bucket.grant_read(self.collect_insider_transactions_fn, "config/*")
        data_bucket.grant_write(
            self.collect_insider_transactions_fn, "insider_transactions/*"
        )
        self.collect_insider_transactions_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
            )
        )

        # ── collect-analyst-data ─────────────────────────────────────────────
        self.collect_analyst_data_fn = lambda_.Function(
            self,
            "CollectAnalystData",
            function_name="market-data-collect-analyst-data",
            runtime=PYTHON_RUNTIME,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset(f"{LAMBDAS_PATH}/collect_analyst_data"),
            layers=all_layers,
            memory_size=512,
            timeout=cdk.Duration.minutes(15),
            environment={**common_env},
        )
        data_bucket.grant_read(self.collect_analyst_data_fn, "config/*")
        data_bucket.grant_write(self.collect_analyst_data_fn, "analyst/*")
        self.collect_analyst_data_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
            )
        )

        # ── collect-financials ────────────────────────────────────────────────
        self.collect_financials_fn = lambda_.Function(
            self,
            "CollectFinancials",
            function_name="market-data-collect-financials",
            runtime=PYTHON_RUNTIME,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset(f"{LAMBDAS_PATH}/collect_financials"),
            layers=all_layers,
            memory_size=1024,
            timeout=cdk.Duration.minutes(15),
            environment={**common_env},
        )
        data_bucket.grant_read(self.collect_financials_fn, "config/*")
        data_bucket.grant_write(self.collect_financials_fn, "financials/*")
        self.collect_financials_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
            )
        )

        # ── emit-metrics ──────────────────────────────────────────────────────
        self.emit_metrics_fn = lambda_.Function(
            self,
            "EmitMetrics",
            function_name="market-data-emit-metrics",
            runtime=PYTHON_RUNTIME,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset(f"{LAMBDAS_PATH}/emit_metrics"),
            layers=all_layers,
            memory_size=128,
            timeout=cdk.Duration.minutes(15),
            environment={**common_env},
        )
        self.emit_metrics_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
            )
        )

        # ── refresh-tickers (daily before market open) ─────────────────────────
        self.refresh_tickers_fn = lambda_.Function(
            self,
            "RefreshTickers",
            function_name="market-data-refresh-tickers",
            runtime=PYTHON_RUNTIME,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset(f"{LAMBDAS_PATH}/refresh_tickers"),
            layers=all_layers,
            memory_size=512,
            timeout=cdk.Duration.minutes(5),
            environment={**common_env},
        )
        data_bucket.grant_read_write(self.refresh_tickers_fn, "config/tickers/*")

        # Schedule: run daily at 12:00 UTC (before any market opens)
        events.Rule(
            self,
            "RefreshTickersSchedule",
            rule_name="market-data-refresh-tickers-daily",
            schedule=events.Schedule.cron(
                hour="12",
                minute="0",
                week_day="MON-FRI",
            ),
            description="Refresh all exchange ticker lists daily before market open",
            targets=[events_targets.LambdaFunction(self.refresh_tickers_fn)],
        )

        # ── ECS Fargate — Fundamentals Collection (once daily) ─────────────────
        # VPC — use default VPC to avoid extra costs
        vpc = ec2.Vpc.from_lookup(self, "DefaultVpc", is_default=True)

        # ECS Cluster
        self.ecs_cluster = ecs.Cluster(
            self,
            "FundamentalsCluster",
            cluster_name="market-data-fundamentals",
            vpc=vpc,
        )

        # Build container image from Dockerfile
        fundamentals_image = ecs.ContainerImage.from_asset("../ecs/fundamentals")

        # Task definition — small: 0.25 vCPU, 0.5 GB RAM
        self.fundamentals_task_def = ecs.FargateTaskDefinition(
            self,
            "FundamentalsTaskDef",
            family="market-data-fundamentals",
            cpu=256,  # 0.25 vCPU
            memory_limit_mib=512,  # 0.5 GB
        )

        # Grant S3 access to the task role
        data_bucket.grant_read(self.fundamentals_task_def.task_role, "config/*")
        data_bucket.grant_read_write(
            self.fundamentals_task_def.task_role, "fundamentals/*"
        )

        # Add container
        self.fundamentals_task_def.add_container(
            "FundamentalsContainer",
            image=fundamentals_image,
            environment={
                "DATA_BUCKET": data_bucket.bucket_name,
                "AWS_REGION": self.region,
            },
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="fundamentals",
                log_retention=logs.RetentionDays.TWO_WEEKS,
            ),
        )

        # EventBridge scheduling is handled by Step Functions state machines
        # in orchestration_stack.py — no direct EventBridge→Fargate rules here

        # ── ECS Fargate — Insider Transactions (once daily, parallel per exchange) ─
        insider_image = ecs.ContainerImage.from_asset("../ecs/insider_transactions")

        self.insider_task_def = ecs.FargateTaskDefinition(
            self,
            "InsiderTaskDef",
            family="market-data-insider-transactions",
            cpu=256,
            memory_limit_mib=512,
        )

        data_bucket.grant_read(self.insider_task_def.task_role, "config/*")
        data_bucket.grant_read_write(
            self.insider_task_def.task_role, "insider_transactions/*"
        )

        self.insider_task_def.add_container(
            "InsiderContainer",
            image=insider_image,
            environment={
                "DATA_BUCKET": data_bucket.bucket_name,
                "AWS_REGION": self.region,
                "EXCHANGE": "NASDAQ",  # default, overridden per task
            },
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="insider-transactions",
                log_retention=logs.RetentionDays.TWO_WEEKS,
            ),
        )

        # ── ECS Fargate — Financials (weekly, parallel per exchange) ─────────────
        financials_image = ecs.ContainerImage.from_asset("../ecs/financials")

        self.financials_task_def = ecs.FargateTaskDefinition(
            self,
            "FinancialsTaskDef",
            family="market-data-financials-ecs",
            cpu=256,
            memory_limit_mib=512,
        )

        data_bucket.grant_read(self.financials_task_def.task_role, "config/*")
        data_bucket.grant_read_write(self.financials_task_def.task_role, "financials/*")

        self.financials_task_def.add_container(
            "FinancialsContainer",
            image=financials_image,
            environment={
                "DATA_BUCKET": data_bucket.bucket_name,
                "AWS_REGION": self.region,
                "EXCHANGE": "NASDAQ",
            },
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="financials",
                log_retention=logs.RetentionDays.TWO_WEEKS,
            ),
        )

        # ── ECS Fargate — Corporate Actions (daily, parallel per exchange) ────
        corporate_actions_image = ecs.ContainerImage.from_asset(
            "../ecs/corporate_actions"
        )

        self.corporate_actions_task_def = ecs.FargateTaskDefinition(
            self,
            "CorporateActionsTaskDef",
            family="market-data-corporate-actions-ecs",
            cpu=256,
            memory_limit_mib=512,
        )

        data_bucket.grant_read(self.corporate_actions_task_def.task_role, "config/*")
        data_bucket.grant_read_write(
            self.corporate_actions_task_def.task_role, "corporate_actions/*"
        )

        self.corporate_actions_task_def.add_container(
            "CorporateActionsContainer",
            image=corporate_actions_image,
            environment={
                "DATA_BUCKET": data_bucket.bucket_name,
                "AWS_REGION": self.region,
                "EXCHANGE": "NASDAQ",
            },
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="corporate-actions",
                log_retention=logs.RetentionDays.TWO_WEEKS,
            ),
        )

        # ── ECS Fargate — Analyst Data (daily, parallel per exchange) ─────────
        analyst_image = ecs.ContainerImage.from_asset("../ecs/analyst_data")

        self.analyst_task_def = ecs.FargateTaskDefinition(
            self,
            "AnalystTaskDef",
            family="market-data-analyst-ecs",
            cpu=256,
            memory_limit_mib=512,
        )

        data_bucket.grant_read(self.analyst_task_def.task_role, "config/*")
        data_bucket.grant_read_write(self.analyst_task_def.task_role, "analyst/*")

        self.analyst_task_def.add_container(
            "AnalystContainer",
            image=analyst_image,
            environment={
                "DATA_BUCKET": data_bucket.bucket_name,
                "AWS_REGION": self.region,
                "EXCHANGE": "NASDAQ",
            },
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="analyst-data",
                log_retention=logs.RetentionDays.TWO_WEEKS,
            ),
        )

        # ── Outputs ───────────────────────────────────────────────────────────
        cdk.CfnOutput(
            self,
            "CollectStocksFnArn",
            value=self.collect_stocks_fn.function_arn,
        )
        cdk.CfnOutput(
            self,
            "CollectNewsFnArn",
            value=self.collect_news_fn.function_arn,
        )
        cdk.CfnOutput(
            self,
            "CollectInsiderTransactionsFnArn",
            value=self.collect_insider_transactions_fn.function_arn,
        )
        cdk.CfnOutput(
            self, "FundamentalsClusterArn", value=self.ecs_cluster.cluster_arn
        )
        cdk.CfnOutput(
            self,
            "FundamentalsTaskDefArn",
            value=self.fundamentals_task_def.task_definition_arn,
        )
        cdk.CfnOutput(
            self, "InsiderTaskDefArn", value=self.insider_task_def.task_definition_arn
        )
        cdk.CfnOutput(
            self, "RefreshTickersFnArn", value=self.refresh_tickers_fn.function_arn
        )
