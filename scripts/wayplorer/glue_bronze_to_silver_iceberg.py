import sys
from functools import reduce

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F, types as T


ENTITY_SCHEMAS = {
    "account": [
        ("applicationId", T.StringType(), True),
        ("accountId", T.StringType(), True),
        ("clientId", T.StringType(), True),
        ("accountName", T.StringType(), False),
        ("industry", T.StringType(), False),
        ("annualRevenue", T.DoubleType(), False),
        ("numberOfEmployees", T.IntegerType(), False),
        ("accountType", T.StringType(), False),
        ("cdc_timestamp", T.TimestampType(), True),
        ("operationType", T.StringType(), True),
    ],
    "lead": [
        ("applicationId", T.StringType(), True),
        ("leadId", T.StringType(), True),
        ("accountId", T.StringType(), False),
        ("clientId", T.StringType(), True),
        ("firstName", T.StringType(), False),
        ("lastName", T.StringType(), False),
        ("email", T.StringType(), False),
        ("company", T.StringType(), False),
        ("leadSource", T.StringType(), False),
        ("leadStatus", T.StringType(), False),
        ("createdDate", T.TimestampType(), False),
        ("cdc_timestamp", T.TimestampType(), True),
        ("operationType", T.StringType(), True),
    ],
    "opportunity": [
        ("applicationId", T.StringType(), True),
        ("opportunityId", T.StringType(), True),
        ("leadId", T.StringType(), False),
        ("accountId", T.StringType(), False),
        ("clientId", T.StringType(), True),
        ("stage", T.StringType(), False),
        ("probability", T.IntegerType(), False),
        ("amount", T.DoubleType(), False),
        ("createdDate", T.TimestampType(), False),
        ("cdc_timestamp", T.TimestampType(), True),
        ("operationType", T.StringType(), True),
    ],
    "contract": [
        ("applicationId", T.StringType(), True),
        ("contractId", T.StringType(), True),
        ("accountId", T.StringType(), False),
        ("clientId", T.StringType(), True),
        ("contractValue", T.DoubleType(), False),
        ("contractStatus", T.StringType(), False),
        ("contractStartDate", T.DateType(), False),
        ("contractEndDate", T.DateType(), False),
        ("cdc_timestamp", T.TimestampType(), True),
        ("operationType", T.StringType(), True),
    ],
    "call": [
        ("applicationId", T.StringType(), True),
        ("callId", T.StringType(), True),
        ("accountId", T.StringType(), False),
        ("leadId", T.StringType(), False),
        ("clientId", T.StringType(), True),
        ("direction", T.StringType(), False),
        ("outcome", T.StringType(), False),
        ("durationSeconds", T.IntegerType(), False),
        ("callStartTime", T.TimestampType(), False),
        ("cdc_timestamp", T.TimestampType(), True),
        ("operationType", T.StringType(), True),
    ],
    "email": [
        ("applicationId", T.StringType(), True),
        ("emailId", T.StringType(), True),
        ("accountId", T.StringType(), False),
        ("leadId", T.StringType(), False),
        ("clientId", T.StringType(), True),
        ("subject", T.StringType(), False),
        ("emailStatus", T.StringType(), False),
        ("sentTime", T.TimestampType(), False),
        ("cdc_timestamp", T.TimestampType(), True),
        ("operationType", T.StringType(), True),
    ],
    "task": [
        ("applicationId", T.StringType(), True),
        ("taskId", T.StringType(), True),
        ("accountId", T.StringType(), False),
        ("leadId", T.StringType(), False),
        ("clientId", T.StringType(), True),
        ("taskSubject", T.StringType(), False),
        ("taskStatus", T.StringType(), False),
        ("taskPriority", T.StringType(), False),
        ("dueDate", T.DateType(), False),
        ("completedAt", T.TimestampType(), False),
        ("cdc_timestamp", T.TimestampType(), True),
        ("operationType", T.StringType(), True),
    ],
}


def cast_column(name: str, data_type: T.DataType) -> F.Column:
    source = F.col(name)
    if isinstance(data_type, T.TimestampType):
        return F.to_timestamp(source).alias(name)
    if isinstance(data_type, T.DateType):
        return F.to_date(source).alias(name)
    return source.cast(data_type).alias(name)


def required_invalid_condition(required_columns: list[str]) -> F.Column:
    conditions = []
    for column_name in required_columns:
        column = F.col(column_name)
        conditions.append(column.isNull() | (F.trim(column.cast("string")) == ""))
    if not conditions:
        return F.lit(False)
    return reduce(lambda left, right: left | right, conditions)


def main() -> None:
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "ENTITY",
            "INPUT_PATH",
            "ICEBERG_CATALOG",
            "ICEBERG_DATABASE",
            "ICEBERG_TABLE",
            "WAREHOUSE_PATH",
            "QUARANTINE_PATH",
        ],
    )

    entity = args["ENTITY"]
    schema_definition = ENTITY_SCHEMAS[entity]
    required_columns = [name for name, _, required in schema_definition if required]
    target_table = f"{args['ICEBERG_CATALOG']}.{args['ICEBERG_DATABASE']}.{args['ICEBERG_TABLE']}"

    spark = (
        SparkSession.builder.appName(args["JOB_NAME"])
        .config(
            f"spark.sql.catalog.{args['ICEBERG_CATALOG']}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            f"spark.sql.catalog.{args['ICEBERG_CATALOG']}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config(
            f"spark.sql.catalog.{args['ICEBERG_CATALOG']}.warehouse",
            args["WAREHOUSE_PATH"],
        )
        .config(
            f"spark.sql.catalog.{args['ICEBERG_CATALOG']}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .getOrCreate()
    )

    raw_df = spark.read.option("multiLine", False).json(args["INPUT_PATH"])
    selected_columns = [cast_column(name, data_type) for name, data_type, _ in schema_definition]
    typed_df = raw_df.select(*selected_columns)

    with_partitions = (
        typed_df.withColumn("year", F.year("cdc_timestamp"))
        .withColumn("month", F.month("cdc_timestamp"))
        .withColumn("day", F.dayofmonth("cdc_timestamp"))
        .withColumn("hour", F.hour("cdc_timestamp"))
        .withColumn("ingested_at", F.current_timestamp())
    )

    invalid_condition = required_invalid_condition(required_columns)
    good_df = with_partitions.filter(~invalid_condition)
    bad_df = with_partitions.filter(invalid_condition).withColumn("validation_reason", F.lit("missing_required_field"))

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args['ICEBERG_CATALOG']}.{args['ICEBERG_DATABASE']}")

    (
        good_df.write.format("iceberg")
        .mode("append")
        .partitionBy("year", "month", "day", "hour")
        .saveAsTable(target_table)
    )

    if bad_df.take(1):
        (
            bad_df.write.mode("append")
            .partitionBy("year", "month", "day", "hour")
            .parquet(f"{args['QUARANTINE_PATH'].rstrip('/')}/{entity}/")
        )

    print(f"Loaded entity={entity} into {target_table}")
    spark.stop()


if __name__ == "__main__":
    main()
