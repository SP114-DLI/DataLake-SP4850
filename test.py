import boto3
from botocore.config import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://sp114api.loclx.io",   # note http, not https
    aws_access_key_id="SP114",
    aws_secret_access_key="DataLakeImplementation",
    region_name="us-east-1",
    use_ssl=False,
    verify=False,
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
        retries={"max_attempts": 2}
    )
)

print(s3.list_buckets())