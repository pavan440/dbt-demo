import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F


def main() -> None:
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "ICEBERG_CATALOG",
            "GOLD_DATABASE",
            "SOURCE_TABLE",
            "EXPORT_PATH",
            "CLIENT_ID",
            "SNAPSHOT_DATE",
        ],
    )

    source_table = f"{args['ICEBERG_CATALOG']}.{args['GOLD_DATABASE']}.{args['SOURCE_TABLE']}"

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
            f"spark.sql.catalog.{args['ICEBERG_CATALOG']}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .getOrCreate()
    )

    df = spark.table(source_table)

    if args["CLIENT_ID"] != "ALL":
        df = df.filter(F.col("client_id") == F.lit(args["CLIENT_ID"]))

    if "feature_date" in df.columns:
        df = df.filter(F.col("feature_date") == F.to_date(F.lit(args["SNAPSHOT_DATE"])))

    export_df = (
        df.withColumn("exported_at", F.current_timestamp())
        .withColumn("export_client_scope", F.lit(args["CLIENT_ID"]))
        .withColumn("export_snapshot_date", F.lit(args["SNAPSHOT_DATE"]))
    )

    target_path = (
        f"{args['EXPORT_PATH'].rstrip('/')}/"
        f"client_id={args['CLIENT_ID']}/"
        f"feature_date={args['SNAPSHOT_DATE']}/"
    )

    (
        export_df.coalesce(1)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(target_path)
    )

    print(f"Exported {source_table} to {target_path}")
    spark.stop()


if __name__ == "__main__":
    main()
