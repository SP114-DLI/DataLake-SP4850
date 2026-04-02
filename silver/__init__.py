"""Silver layer - data cleaning, deduplication, and transformation.

This layer contains code to:
- Download raw JSONL listings from the lakeraw S3 bucket
- Flatten nested JSON (dealer, monthlyPaymentEstimate) into tabular columns
- Deduplicate listings by VIN
- Convert to Parquet format and upload to the lakesilver S3 bucket

Usage:
    python -m silver.transform
"""
