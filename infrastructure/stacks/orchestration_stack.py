"""
OrchestrationStack — Step Functions Express Workflow + EventBridge Scheduler.
"""

from __future__ import annotations

import json
import aws_cdk as cdk
from aws_cdk import (
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_scheduler as scheduler,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
)
from constructs import Construct


class OrchestrationStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        check_market_hours_fn: lambda_.Function,
        collect_stocks_fn: lambda_.Function,
        collect_news_fn: lambda_.Function,
        update_glue_catalog_fn: lambda_.Function,
        emit_metrics_fn: lambda_.Function,
        detect_signals_fn: lambda_.Function,
        ecs_cluster: "ecs.Cluster",
        financials_task_def: "ecs.FargateTaskDefinition",
        corporate_actions_task_def: "ecs.FargateTaskDefinition",
        analyst_task_def: "ecs.FargateTaskDefinition",
        fundamentals_task_def: "ecs.FargateTaskDefinition",
        insider_task_def: "ecs.FargateTaskDefinition",
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── SNS Alert topic ───────────────────────────────────────────────────
        alert_topic = sns.Topic(self, "AlertTopic", topic_name="market-data-alerts")
        # Add your email here after deploy:
        # alert_topic.add_subscription(subscriptions.EmailSubscription("you@example.com"))

        # ── Step Functions state machine ──────────────────────────────────────
        log_group = logs.LogGroup(
            self,
            "SfnLogGroup",
            log_group_name="/aws/states/market-data-pipeline",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        # State: CheckMarketHours
        check_hours = sfn_tasks.LambdaInvoke(
            self,
            "CheckMarketHours",
            lambda_function=check_market_hours_fn,
            payload=sfn.TaskInput.from_json_path_at("$"),
            result_selector={
                "open_exchanges.$": "$.Payload.open_exchanges",
                "ready.$": "$.Payload.ready",
                "chunks.$": "$.Payload.chunks",
            },
            result_path="$.market_check",
            retry_on_service_exceptions=True,
        )

        # Choice: any exchanges open?
        no_exchanges_open = sfn.Succeed(self, "NoExchangesOpen")
        check_open = sfn.Choice(self, "AnyExchangesOpen?")

        # State: CollectStocks (Map over open exchanges)
        collect_stocks_job = sfn_tasks.LambdaInvoke(
            self,
            "CollectStocksForExchange",
            lambda_function=collect_stocks_fn,
            payload=sfn.TaskInput.from_json_path_at("$"),
            result_path="$.stock_result",
            retry_on_service_exceptions=False,
        )
        # Retry once only. A chunk is ~400 tickers, so a retry is cheap (the old
        # per-exchange retry re-ran thousands of tickers up to 3 more times).
        collect_stocks_job.add_retry(
            errors=["States.TaskFailed", "Lambda.ServiceException"],
            interval=cdk.Duration.seconds(5),
            max_attempts=1,
            backoff_rate=2.0,
        )
        # A chunk that still fails is recorded and skipped, so it can't abort
        # the other chunks or the steps after collection.
        chunk_failed = sfn.Pass(self, "ChunkFailed")
        collect_stocks_job.add_catch(
            chunk_failed,
            errors=["States.ALL"],
            result_path="$.chunk_error",
        )

        # One Map item per ticker chunk (planned by check-market-hours)
        collect_stocks_map = sfn.Map(
            self,
            "CollectStocksPerExchange",
            items_path="$.market_check.chunks",
            max_concurrency=10,
            result_path="$.stocks_results",
            parameters={
                "exchange.$": "$$.Map.Item.Value.exchange",
                "offset.$": "$$.Map.Item.Value.offset",
                "limit.$": "$$.Map.Item.Value.limit",
                "start_date.$": "$$.Map.Item.Value.start_date",
                "end_date.$": "$$.Map.Item.Value.end_date",
            },
        )
        collect_stocks_map.item_processor(collect_stocks_job)

        # State: Run stocks only (news has its own dedicated pipeline below)
        parallel_collect = sfn.Parallel(
            self,
            "ParallelCollect",
            result_path="$.parallel_results",
        )
        parallel_collect.branch(collect_stocks_map)

        # State: EmitMetrics (always runs — success or failure path)
        emit_metrics = sfn_tasks.LambdaInvoke(
            self,
            "EmitMetrics",
            lambda_function=emit_metrics_fn,
            payload=sfn.TaskInput.from_json_path_at("$"),
            result_path="$.metrics_result",
        )

        # Error handler — catch all, route to EmitMetrics with error context
        pipeline_failed = sfn.Fail(self, "PipelineFailed", error="PipelineError")

        emit_metrics_on_error = sfn_tasks.LambdaInvoke(
            self,
            "EmitMetricsOnError",
            lambda_function=emit_metrics_fn,
            payload=sfn.TaskInput.from_object(
                {
                    # add_catch below stores the error object at $.error
                    "error.$": "$.error.Error",
                    "cause.$": "$.error.Cause",
                    "run_timestamp.$": "$$.Execution.StartTime",
                    "status": "FAILED",
                }
            ),
        )
        emit_metrics_on_error.next(pipeline_failed)

        parallel_collect.add_catch(emit_metrics_on_error, result_path="$.error")

        # State: MarkCollected — record each exchange-day whose chunks all
        # succeeded, so later 2-hourly checks skip it; failures stay pending.
        mark_collected = sfn_tasks.LambdaInvoke(
            self,
            "MarkCollected",
            lambda_function=check_market_hours_fn,
            payload=sfn.TaskInput.from_object(
                {
                    "action": "mark",
                    "ready.$": "$.market_check.ready",
                    "stocks_results.$": "$.parallel_results[0].stocks_results",
                }
            ),
            result_path="$.mark_result",
            result_selector={
                "marked.$": "$.Payload.marked",
                "pending_retry.$": "$.Payload.pending_retry",
            },
        )

        # State: RegisterPartitions — make the new daily bars queryable in
        # Athena right away instead of waiting for the next scheduled glue sync.
        # A week covers catch-up runs after missed days.
        register_partitions = sfn_tasks.LambdaInvoke(
            self,
            "RegisterPartitions",
            lambda_function=update_glue_catalog_fn,
            payload=sfn.TaskInput.from_object({"lookback_days": 7}),
            result_path="$.glue_result",
            result_selector={"partitions_added.$": "$.Payload.partitions_added"},
        )
        # Data is already in S3 and the scheduled sync will register it later,
        # so a catalog hiccup shouldn't fail the run.
        register_partitions.add_catch(
            emit_metrics, errors=["States.ALL"], result_path="$.glue_error"
        )

        # Chain the happy path
        definition = check_hours.next(
            check_open.when(
                sfn.Condition.is_present("$.market_check.open_exchanges[0]"),
                parallel_collect.next(mark_collected).next(register_partitions).next(emit_metrics),
            ).otherwise(no_exchanges_open)
        )

        state_machine = sfn.StateMachine(
            self,
            "MarketDataPipeline",
            state_machine_name="market-data-pipeline-v2",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            state_machine_type=sfn.StateMachineType.STANDARD,
            timeout=cdk.Duration.minutes(60),
            logs=sfn.LogOptions(
                destination=log_group,
                level=sfn.LogLevel.ERROR,
                include_execution_data=False,
            ),
            tracing_enabled=True,
        )

        # ── EventBridge Scheduler ─────────────────────────────────────────────
        scheduler_role = iam.Role(
            self,
            "SchedulerRole",
            assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),
        )
        state_machine.grant_start_execution(scheduler_role)

        scheduler.CfnSchedule(
            self,
            "MarketDataSchedule",
            name="market-data-hourly",
            # Every 2 hours, every day: check-market-hours collects each exchange's
            # daily bar once, >= 2h after its local close. Most runs find nothing
            # ready and end at NoExchangesOpen. Weekend runs pick up Friday's US
            # close (Sat 00:00-02:00 UTC) and catch up any missed day.
            schedule_expression="cron(0 0/2 * * ? *)",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="FLEXIBLE",
                maximum_window_in_minutes=5,
            ),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=state_machine.state_machine_arn,
                role_arn=scheduler_role.role_arn,
                input=json.dumps({"source": "eventbridge-scheduler"}),
            ),
            state="ENABLED",
            description="Trigger market data collection covering all global market hours",
        )

        # ── CloudWatch Alarms ─────────────────────────────────────────────────
        sfn_failures = cloudwatch.Metric(
            namespace="AWS/States",
            metric_name="ExecutionsFailed",
            dimensions_map={"StateMachineArn": state_machine.state_machine_arn},
            statistic="Sum",
            period=cdk.Duration.minutes(30),
        )

        cloudwatch.Alarm(
            self,
            "PipelineFailureAlarm",
            alarm_name="MarketDataPipelineFailed",
            metric=sfn_failures,
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            alarm_description="Step Functions execution failed",
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        ).add_alarm_action(cw_actions.SnsAction(alert_topic))

        stocks_failed_metric = cloudwatch.Metric(
            namespace="MarketData",
            metric_name="StocksFailed",
            statistic="Sum",
            period=cdk.Duration.minutes(30),
        )
        cloudwatch.Alarm(
            self,
            "HighStockFailureAlarm",
            alarm_name="MarketDataHighStockFailureRate",
            metric=stocks_failed_metric,
            threshold=100,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            alarm_description="More than 100 stock tickers failed in a single run",
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        ).add_alarm_action(cw_actions.SnsAction(alert_topic))

        # ── News Pipeline State Machine (always-on, independent of market hours) ─
        # Weekdays:  every 30 minutes around the clock
        # Weekends:  every 4 hours (news still matters but less volume)

        news_log_group = logs.LogGroup(
            self,
            "NewsSfnLogGroup",
            log_group_name="/aws/states/market-data-news",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        collect_news_state = sfn_tasks.LambdaInvoke(
            self,
            "CollectNewsState",
            lambda_function=collect_news_fn,
            payload=sfn.TaskInput.from_object(
                {
                    "run_timestamp.$": "$$.Execution.StartTime",
                    "date.$": "States.Format('{}', $$.Execution.StartTime)",
                }
            ),
            result_path="$.news_result",
            retry_on_service_exceptions=False,
        )
        collect_news_state.add_retry(
            errors=["States.TaskFailed", "Lambda.ServiceException"],
            interval=cdk.Duration.seconds(5),
            max_attempts=3,
            backoff_rate=2.0,
        )

        news_definition = collect_news_state

        news_state_machine = sfn.StateMachine(
            self,
            "NewsPipeline",
            state_machine_name="market-data-news-pipeline-v2",
            definition_body=sfn.DefinitionBody.from_chainable(news_definition),
            state_machine_type=sfn.StateMachineType.STANDARD,
            timeout=cdk.Duration.minutes(10),
            logs=sfn.LogOptions(
                destination=news_log_group,
                level=sfn.LogLevel.ERROR,
                include_execution_data=False,
            ),
            tracing_enabled=True,
        )

        news_state_machine.grant_start_execution(scheduler_role)

        # Weekday schedule — every hour Mon–Fri
        scheduler.CfnSchedule(
            self,
            "NewsWeekdaySchedule",
            name="market-data-news-4h",
            schedule_expression="rate(4 hours)",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="FLEXIBLE",
                maximum_window_in_minutes=30,
            ),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=news_state_machine.state_machine_arn,
                role_arn=scheduler_role.role_arn,
                input=json.dumps({"source": "eventbridge-scheduler-news-weekday"}),
            ),
            state="ENABLED",
            description="Collect financial news every 4 hours",
        )

        # Weekend schedule — every 4 hours Sat-Sun
        scheduler.CfnSchedule(
            self,
            "NewsWeekendSchedule",
            name="market-data-news-weekend",
            schedule_expression="rate(4 hours)",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="OFF",
            ),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=news_state_machine.state_machine_arn,
                role_arn=scheduler_role.role_arn,
                input=json.dumps({"source": "eventbridge-scheduler-news-weekend"}),
            ),
            state="ENABLED",
            description="Collect financial news every 4 hours on weekends",
        )

        # ── Corporate Actions State Machine (daily) ───────────────────────────
        # Runs once per day at 07:00 UTC (before any exchange opens).
        # Fan-out over all exchanges in parallel via ECS Fargate (no Lambda timeout).

        ca_log_group = logs.LogGroup(
            self,
            "CaSfnLogGroup",
            log_group_name="/aws/states/market-data-corporate-actions",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        # All known exchange codes
        all_exchanges = [
            "NASDAQ",
            "NYSE",
            "LSE",
            "XETRA",
            "EURONEXT",
            "EURONEXT_AMS",
            "EURONEXT_BRU",
            "EURONEXT_LIS",
            "BORSA_ITALIANA",
            "TSE",
            "HKEX",
        ]

        # VPC for ECS tasks
        vpc = ec2.Vpc.from_lookup(self, "OrchestrVpc", is_default=True)

        # One security group for all per-exchange ECS tasks (outbound only).
        ecs_tasks_sg = ec2.SecurityGroup(
            self,
            "EcsExchangeTasksSecurityGroup",
            vpc=ecs_cluster.vpc,
            description="Per-exchange ECS collection tasks (outbound only)",
            allow_all_outbound=True,
        )

        collect_ca_task, collect_ca_task_policy = self._exchange_ecs_task(
            "CollectCorporateActionsForExchange", ecs_cluster, corporate_actions_task_def, ecs_tasks_sg
        )

        ca_map = sfn.Map(
            self,
            "CollectCorporateActionsPerExchange",
            items_path="$.exchanges",
            max_concurrency=7,
            result_path="$.ca_results",
            parameters={
                "exchange.$": "$$.Map.Item.Value",
            },
        )
        ca_map.item_processor(collect_ca_task)

        ca_definition = ca_map

        ca_state_machine = sfn.StateMachine(
            self,
            "CorporateActionsPipeline",
            state_machine_name="market-data-corporate-actions-v2",
            definition_body=sfn.DefinitionBody.from_chainable(ca_definition),
            state_machine_type=sfn.StateMachineType.STANDARD,
            timeout=cdk.Duration.minutes(120),  # 2 hours for ECS tasks
            logs=sfn.LogOptions(
                destination=ca_log_group,
                level=sfn.LogLevel.ERROR,
                include_execution_data=False,
            ),
            tracing_enabled=True,
        )

        # Daily schedule — 07:00 UTC, every weekday
        ca_state_machine.grant_start_execution(scheduler_role)

        scheduler.CfnSchedule(
            self,
            "CorporateActionsSchedule",
            name="market-data-corporate-actions-daily",
            # cron: minute=0, hour=7, every day Mon–Fri
            schedule_expression="cron(0 7 ? * MON-FRI *)",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="OFF",
            ),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=ca_state_machine.state_machine_arn,
                role_arn=scheduler_role.role_arn,
                input=json.dumps(
                    {
                        "source": "eventbridge-scheduler-daily",
                        "exchanges": all_exchanges,
                    }
                ),
            ),
            state="ENABLED",
            description="Collect stock splits and dividends once per day before market open",
        )

        # ── Analyst Data State Machine (daily, 07:30 UTC Mon–Fri) ─────────────
        # Runs 30 minutes after corporate actions — after splits/dividends are
        # recorded, collect price targets, recommendations, and earnings dates.

        analyst_log_group = logs.LogGroup(
            self,
            "AnalystSfnLogGroup",
            log_group_name="/aws/states/market-data-analyst",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        collect_analyst_task, collect_analyst_task_policy = self._exchange_ecs_task(
            "CollectAnalystDataForExchange", ecs_cluster, analyst_task_def, ecs_tasks_sg
        )

        analyst_map = sfn.Map(
            self,
            "CollectAnalystDataPerExchange",
            items_path="$.exchanges",
            # All exchanges at once: run time = slowest exchange (NASDAQ ~1.6h)
            # instead of queueing TSE/HKEX behind it. Each task has its own IP,
            # and Fargate bills task time, so parallelism doesn't add cost.
            max_concurrency=11,
            result_path="$.analyst_results",
            parameters={
                "exchange.$": "$$.Map.Item.Value",
            },
        )
        analyst_map.item_processor(collect_analyst_task)

        analyst_definition = analyst_map

        analyst_state_machine = sfn.StateMachine(
            self,
            "AnalystDataPipeline",
            state_machine_name="market-data-analyst-v2",
            definition_body=sfn.DefinitionBody.from_chainable(analyst_definition),
            state_machine_type=sfn.StateMachineType.STANDARD,
            timeout=cdk.Duration.hours(4),
            logs=sfn.LogOptions(
                destination=analyst_log_group,
                level=sfn.LogLevel.ERROR,
                include_execution_data=False,
            ),
            tracing_enabled=True,
        )

        analyst_state_machine.grant_start_execution(scheduler_role)

        scheduler.CfnSchedule(
            self,
            "AnalystDataSchedule",
            name="market-data-analyst-daily",
            schedule_expression="cron(30 7 ? * MON-FRI *)",  # 07:30 UTC Mon–Fri
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="OFF",
            ),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=analyst_state_machine.state_machine_arn,
                role_arn=scheduler_role.role_arn,
                input=json.dumps(
                    {
                        "source": "eventbridge-scheduler-analyst",
                        "exchanges": all_exchanges,
                    }
                ),
            ),
            state="ENABLED",
            description="Collect analyst price targets, recommendations, and earnings dates daily",
        )

        # ── Financials State Machine (weekly, Sunday 06:00 UTC) ───────────────
        # Financial statements are filed quarterly — weekly collection is plenty.
        # Sunday 06:00 UTC is well outside all market hours globally.

        financials_log_group = logs.LogGroup(
            self,
            "FinancialsSfnLogGroup",
            log_group_name="/aws/states/market-data-financials",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        collect_financials_task, collect_financials_task_policy = self._exchange_ecs_task(
            "CollectFinancialsForExchange", ecs_cluster, financials_task_def, ecs_tasks_sg
        )

        financials_map = sfn.Map(
            self,
            "CollectFinancialsPerExchange",
            items_path="$.exchanges",
            # Was 3: NASDAQ alone takes ~1.9h, so HKEX never started in time and
            # every weekly run timed out. Run all exchanges at once.
            max_concurrency=11,
            result_path="$.financials_results",
            parameters={
                "exchange.$": "$$.Map.Item.Value",
            },
        )
        financials_map.item_processor(collect_financials_task)

        financials_definition = financials_map

        financials_state_machine = sfn.StateMachine(
            self,
            "FinancialsPipeline",
            state_machine_name="market-data-financials-v2",
            definition_body=sfn.DefinitionBody.from_chainable(financials_definition),
            state_machine_type=sfn.StateMachineType.STANDARD,
            timeout=cdk.Duration.hours(4),  # longest exchange ~1.9h, with headroom
            logs=sfn.LogOptions(
                destination=financials_log_group,
                level=sfn.LogLevel.ERROR,
                include_execution_data=False,
            ),
            tracing_enabled=True,
        )

        financials_state_machine.grant_start_execution(scheduler_role)

        scheduler.CfnSchedule(
            self,
            "FinancialsSchedule",
            name="market-data-financials-weekly",
            schedule_expression="cron(0 6 ? * SUN *)",  # 06:00 UTC every Sunday
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="OFF",
            ),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=financials_state_machine.state_machine_arn,
                role_arn=scheduler_role.role_arn,
                input=json.dumps(
                    {
                        "source": "eventbridge-scheduler-financials",
                        "exchanges": all_exchanges,
                    }
                ),
            ),
            state="ENABLED",
            description="Collect full financial statements (income, balance sheet, cashflow) weekly",
        )

        # ── Fundamentals State Machine (daily after market close) ────────────
        fundamentals_log_group = logs.LogGroup(
            self,
            "FundamentalsSfnLogGroup",
            log_group_name="/aws/states/market-data-fundamentals",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        collect_fundamentals_task, collect_fundamentals_task_policy = self._exchange_ecs_task(
            "CollectFundamentalsForExchange", ecs_cluster, fundamentals_task_def, ecs_tasks_sg
        )

        fundamentals_map = sfn.Map(
            self,
            "CollectFundamentalsPerExchange",
            items_path="$.exchanges",
            # One exchange per task (largest ~3.8k tickers); all at once so the
            # run finishes before detect-signals at 23:00.
            max_concurrency=11,
            result_path=sfn.JsonPath.DISCARD,
            parameters={"exchange.$": "$$.Map.Item.Value"},
        )
        fundamentals_map.item_processor(collect_fundamentals_task)

        fundamentals_state_machine = sfn.StateMachine(
            self,
            "FundamentalsPipeline",
            state_machine_name="market-data-fundamentals-v2",
            definition_body=sfn.DefinitionBody.from_chainable(fundamentals_map),
            state_machine_type=sfn.StateMachineType.STANDARD,
            timeout=cdk.Duration.minutes(120),
            logs=sfn.LogOptions(
                destination=fundamentals_log_group,
                level=sfn.LogLevel.ERROR,
                include_execution_data=False,
            ),
            tracing_enabled=True,
        )
        fundamentals_state_machine.grant_start_execution(scheduler_role)

        scheduler.CfnSchedule(
            self,
            "FundamentalsSchedule",
            name="market-data-fundamentals-daily",
            schedule_expression="cron(0 22 ? * MON-FRI *)",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="OFF"
            ),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=fundamentals_state_machine.state_machine_arn,
                role_arn=scheduler_role.role_arn,
                input=json.dumps(
                    {
                        "source": "eventbridge-scheduler-fundamentals",
                        "exchanges": all_exchanges,
                    }
                ),
            ),
            state="ENABLED",
            description="Collect fundamentals daily after market close",
        )

        # ── Insider Transactions State Machine (daily, parallel per exchange) ─
        insider_log_group = logs.LogGroup(
            self,
            "InsiderFargateSfnLogGroup",
            log_group_name="/aws/states/market-data-insider-fargate",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        insider_exchanges = ["NASDAQ", "NYSE", "LSE", "TSE", "HKEX"]

        collect_insider_fargate_task, collect_insider_fargate_task_policy = self._exchange_ecs_task(
            "CollectInsiderForExchange", ecs_cluster, insider_task_def, ecs_tasks_sg
        )

        insider_fargate_map = sfn.Map(
            self,
            "CollectInsiderPerExchange",
            items_path="$.exchanges",
            max_concurrency=5,
            result_path=sfn.JsonPath.DISCARD,
            parameters={"exchange.$": "$$.Map.Item.Value"},
        )
        insider_fargate_map.item_processor(collect_insider_fargate_task)

        insider_fargate_state_machine = sfn.StateMachine(
            self,
            "InsiderFargatePipeline",
            state_machine_name="market-data-insider-fargate-v2",
            definition_body=sfn.DefinitionBody.from_chainable(insider_fargate_map),
            state_machine_type=sfn.StateMachineType.STANDARD,
            timeout=cdk.Duration.minutes(120),
            logs=sfn.LogOptions(
                destination=insider_log_group,
                level=sfn.LogLevel.ERROR,
                include_execution_data=False,
            ),
            tracing_enabled=True,
        )
        insider_fargate_state_machine.grant_start_execution(scheduler_role)

        scheduler.CfnSchedule(
            self,
            "InsiderFargateSchedule",
            name="market-data-insider-fargate-daily",
            schedule_expression="cron(30 22 ? * MON-FRI *)",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="OFF"
            ),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=insider_fargate_state_machine.state_machine_arn,
                role_arn=scheduler_role.role_arn,
                input=json.dumps(
                    {
                        "source": "eventbridge-scheduler-insider-fargate",
                        "exchanges": insider_exchanges,
                    }
                ),
            ),
            state="ENABLED",
            description="Collect insider transactions daily via ECS Fargate",
        )

        # ── Glue Catalog Partition Sync (standalone, 4x/day) ─────────────────
        # Runs independently of all pipelines. Scans last 30 days of S3 data
        # and registers any missing partitions with Glue so Athena can query them.
        # Schedule: 01:00, 07:00, 13:00, 19:00 UTC (every 6 hours, staggered)

        glue_sync_log_group = logs.LogGroup(
            self,
            "GlueSyncSfnLogGroup",
            log_group_name="/aws/states/market-data-glue-sync",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        glue_sync_task = sfn_tasks.LambdaInvoke(
            self,
            "GlueSyncTask",
            lambda_function=update_glue_catalog_fn,
            payload=sfn.TaskInput.from_object({"lookback_days": 30}),
            result_path="$.glue_result",
        )
        glue_sync_task.add_retry(
            errors=["States.TaskFailed", "Lambda.ServiceException"],
            interval=cdk.Duration.seconds(30),
            max_attempts=2,
            backoff_rate=2.0,
        )

        glue_sync_state_machine = sfn.StateMachine(
            self,
            "GlueSyncPipeline",
            state_machine_name="market-data-glue-sync-v2",
            definition_body=sfn.DefinitionBody.from_chainable(glue_sync_task),
            state_machine_type=sfn.StateMachineType.STANDARD,
            timeout=cdk.Duration.minutes(20),
            logs=sfn.LogOptions(
                destination=glue_sync_log_group,
                level=sfn.LogLevel.ERROR,
                include_execution_data=False,
            ),
            tracing_enabled=True,
        )

        glue_sync_state_machine.grant_start_execution(scheduler_role)

        # 01:00 UTC
        # Weekday: 01:00, 07:00, 13:00, 19:00 UTC Mon–Fri
        scheduler.CfnSchedule(
            self,
            "GlueSyncSchedule01",
            name="market-data-glue-sync-01",
            schedule_expression="cron(0 1 ? * MON-FRI *)",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="OFF",
            ),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=glue_sync_state_machine.state_machine_arn,
                role_arn=scheduler_role.role_arn,
                input=json.dumps({"source": "eventbridge-glue-sync-01"}),
            ),
            state="ENABLED",
            description="Glue partition sync at 01:00 UTC weekdays",
        )

        scheduler.CfnSchedule(
            self,
            "GlueSyncSchedule07",
            name="market-data-glue-sync-07",
            schedule_expression="cron(0 7 ? * MON-FRI *)",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="OFF",
            ),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=glue_sync_state_machine.state_machine_arn,
                role_arn=scheduler_role.role_arn,
                input=json.dumps({"source": "eventbridge-glue-sync-07"}),
            ),
            state="ENABLED",
            description="Glue partition sync at 07:00 UTC weekdays",
        )

        scheduler.CfnSchedule(
            self,
            "GlueSyncSchedule13",
            name="market-data-glue-sync-13",
            schedule_expression="cron(0 13 ? * MON-FRI *)",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="OFF",
            ),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=glue_sync_state_machine.state_machine_arn,
                role_arn=scheduler_role.role_arn,
                input=json.dumps({"source": "eventbridge-glue-sync-13"}),
            ),
            state="ENABLED",
            description="Glue partition sync at 13:00 UTC weekdays",
        )

        scheduler.CfnSchedule(
            self,
            "GlueSyncSchedule19",
            name="market-data-glue-sync-19",
            schedule_expression="cron(0 19 ? * MON-FRI *)",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="OFF",
            ),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=glue_sync_state_machine.state_machine_arn,
                role_arn=scheduler_role.role_arn,
                input=json.dumps({"source": "eventbridge-glue-sync-19"}),
            ),
            state="ENABLED",
            description="Glue partition sync at 19:00 UTC weekdays",
        )

        # Weekend: once on Saturday and once on Sunday at 20:00 UTC
        scheduler.CfnSchedule(
            self,
            "GlueSyncScheduleWeekend",
            name="market-data-glue-sync-weekend",
            schedule_expression="cron(0 20 ? * SAT-SUN *)",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="OFF",
            ),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=glue_sync_state_machine.state_machine_arn,
                role_arn=scheduler_role.role_arn,
                input=json.dumps({"source": "eventbridge-glue-sync-weekend"}),
            ),
            state="ENABLED",
            description="Glue partition sync once daily on weekends at 20:00 UTC",
        )

        # ── Signal Detection State Machine (daily, 23:00 UTC) ────────────────
        # Runs after all data pipelines complete. Queries Athena across all
        # tables, scores stocks on multiple factors, and writes opportunities.

        signals_log_group = logs.LogGroup(
            self,
            "SignalsSfnLogGroup",
            log_group_name="/aws/states/market-data-signals",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        detect_signals_task = sfn_tasks.LambdaInvoke(
            self,
            "DetectSignalsTask",
            lambda_function=detect_signals_fn,
            payload=sfn.TaskInput.from_object({}),
            result_path="$.signals_result",
        )
        detect_signals_task.add_retry(
            errors=["States.TaskFailed", "Lambda.ServiceException"],
            interval=cdk.Duration.seconds(30),
            max_attempts=2,
            backoff_rate=2.0,
        )

        signals_state_machine = sfn.StateMachine(
            self,
            "SignalDetectionPipeline",
            state_machine_name="market-data-signals-v2",
            definition_body=sfn.DefinitionBody.from_chainable(detect_signals_task),
            state_machine_type=sfn.StateMachineType.STANDARD,
            timeout=cdk.Duration.minutes(20),
            logs=sfn.LogOptions(
                destination=signals_log_group,
                level=sfn.LogLevel.ERROR,
                include_execution_data=False,
            ),
            tracing_enabled=True,
        )

        signals_state_machine.grant_start_execution(scheduler_role)

        scheduler.CfnSchedule(
            self,
            "SignalDetectionSchedule",
            name="market-data-signals-daily",
            schedule_expression="cron(0 23 ? * MON-FRI *)",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="OFF",
            ),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=signals_state_machine.state_machine_arn,
                role_arn=scheduler_role.role_arn,
                input=json.dumps({"source": "eventbridge-scheduler-signals"}),
            ),
            state="ENABLED",
            description="Run signal detection daily at 23:00 UTC after all pipelines complete",
        )

        # ── Outputs ───────────────────────────────────────────────────────────
        # Permissions for the per-exchange ECS steps (see _exchange_ecs_task).
        for ecs_state_machine, statements in [
            (ca_state_machine, collect_ca_task_policy),
            (analyst_state_machine, collect_analyst_task_policy),
            (financials_state_machine, collect_financials_task_policy),
            (fundamentals_state_machine, collect_fundamentals_task_policy),
            (insider_fargate_state_machine, collect_insider_fargate_task_policy),
        ]:
            for statement in statements:
                ecs_state_machine.add_to_role_policy(statement)

        cdk.CfnOutput(self, "StateMachineArn", value=state_machine.state_machine_arn)
        cdk.CfnOutput(
            self, "NewsStateMachineArn", value=news_state_machine.state_machine_arn
        )
        cdk.CfnOutput(
            self,
            "CorporateActionsStateMachineArn",
            value=ca_state_machine.state_machine_arn,
        )
        cdk.CfnOutput(
            self,
            "AnalystStateMachineArn",
            value=analyst_state_machine.state_machine_arn,
        )
        cdk.CfnOutput(
            self,
            "FinancialsStateMachineArn",
            value=financials_state_machine.state_machine_arn,
        )
        cdk.CfnOutput(
            self,
            "FundamentalsStateMachineArn",
            value=fundamentals_state_machine.state_machine_arn,
        )
        cdk.CfnOutput(
            self,
            "InsiderFargateStateMachineArn",
            value=insider_fargate_state_machine.state_machine_arn,
        )
        cdk.CfnOutput(
            self,
            "GlueSyncStateMachineArn",
            value=glue_sync_state_machine.state_machine_arn,
        )
        cdk.CfnOutput(self, "AlertTopicArn", value=alert_topic.topic_arn)

    def _exchange_ecs_task(
        self,
        construct_id: str,
        cluster: "ecs.Cluster",
        task_def: "ecs.FargateTaskDefinition",
        security_group: ec2.ISecurityGroup,
    ) -> tuple[sfn.CustomState, list[iam.PolicyStatement]]:
        """
        Run one exchange's ECS task (EXCHANGE=$.exchange) and wait for it.

        Same state EcsRunTask produced, but the task is referenced only by its
        fixed family name. EcsRunTask derived its IAM resource from the task
        definition ARN, which carries the revision, so this stack imported an
        export that changes on every task definition update; CloudFormation
        refuses to update an export in use, so no ECS change could deploy.
        Returns the state and the policy statements its state machine needs.
        """
        family = task_def.family
        state = sfn.CustomState(
            self,
            construct_id,
            state_json={
                "Type": "Task",
                "Resource": f"arn:{self.partition}:states:::ecs:runTask.sync",
                "Parameters": {
                    "Cluster": cluster.cluster_arn,
                    "TaskDefinition": family,  # latest ACTIVE revision
                    "LaunchType": "FARGATE",
                    "PlatformVersion": "LATEST",
                    "NetworkConfiguration": {
                        "AwsvpcConfiguration": {
                            "AssignPublicIp": "ENABLED",
                            "Subnets": [s.subnet_id for s in cluster.vpc.public_subnets],
                            "SecurityGroups": [security_group.security_group_id],
                        }
                    },
                    "Overrides": {
                        "ContainerOverrides": [
                            {
                                "Name": task_def.default_container.container_name,
                                "Environment": [
                                    {"Name": "EXCHANGE", "Value.$": "$.exchange"}
                                ],
                            }
                        ]
                    },
                },
                "ResultPath": None,
                "Retry": [
                    {
                        "ErrorEquals": ["States.TaskFailed"],
                        "IntervalSeconds": 30,
                        "MaxAttempts": 2,
                        "BackoffRate": 2,
                    }
                ],
            },
        )
        statements = [
            iam.PolicyStatement(
                actions=["ecs:RunTask"],
                resources=[
                    f"arn:{self.partition}:ecs:{self.region}:{self.account}"
                    f":task-definition/{family}:*"
                ],
            ),
            iam.PolicyStatement(actions=["ecs:StopTask", "ecs:DescribeTasks"], resources=["*"]),
            iam.PolicyStatement(
                actions=["iam:PassRole"],
                resources=[task_def.task_role.role_arn, task_def.execution_role.role_arn],
            ),
            iam.PolicyStatement(
                actions=["events:PutTargets", "events:PutRule", "events:DescribeRule"],
                resources=[
                    self.format_arn(
                        service="events",
                        resource="rule",
                        resource_name="StepFunctionsGetEventsForECSTaskRule",
                    )
                ],
            ),
        ]
        return state, statements
