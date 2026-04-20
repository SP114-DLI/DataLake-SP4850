"""Print the first row of a local parquet file (quick inspection)."""

import sys
import pandas as pd

DEFAULT_PATH = "downloads/listings_part_001.parquet"


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PATH
    df = pd.read_parquet(path)
    print(df.iloc[0].to_string())


if __name__ == "__main__":
    main()
