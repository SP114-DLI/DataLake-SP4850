"""Silver layer - schema-enforced, deduplicated, quality-checked data.

Modules:
    transform       Raw JSONL -> clean Parquet (with quarantine)
    quality_report  Null/missing statistics across silver data
"""