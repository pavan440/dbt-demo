import argparse
from pathlib import Path

import boto3


def iter_files(root: Path):
    for path in root.rglob("*"):
        if path.is_file():
            yield path


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload local Wayplorer Silver PoC files to S3.")
    parser.add_argument("--local-dir", default="data/silver_poc", help="Local Silver PoC directory.")
    parser.add_argument("--bucket", default="wayplorer-data-lake-449", help="Target S3 bucket.")
    parser.add_argument(
        "--prefix",
        default="wayplorerdbtpoc",
        help="Target S3 prefix for Silver tables.",
    )
    parser.add_argument("--profile", default="learning", help="AWS profile name.")
    parser.add_argument("--region", default="us-east-1", help="AWS region.")
    args = parser.parse_args()

    root = Path(args.local_dir).resolve()
    if not root.exists():
        raise FileNotFoundError(f"Local directory not found: {root}")

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    s3 = session.client("s3")

    uploaded = 0
    for local_file in iter_files(root):
        relative_key = local_file.relative_to(root).as_posix()
        s3_key = f"{args.prefix.rstrip('/')}/{relative_key}"
        s3.upload_file(str(local_file), args.bucket, s3_key)
        uploaded += 1
        print(f"Uploaded s3://{args.bucket}/{s3_key}")

    print(f"Upload complete. Files uploaded: {uploaded}")


if __name__ == "__main__":
    main()

