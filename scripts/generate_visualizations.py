"""
Gold Market Summary — Dataset Visualizations
=============================================
Downloads gold_market_summary.parquet from the lakegold bucket and
produces descriptive graphics for the car-listings dataset.
"""

from pathlib import Path

import boto3
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from botocore.config import Config

from raw.config import ENDPOINT, ACCESS_KEY, SECRET_KEY, REGION, USE_HTTPS, BUCKET_GOLD

# ── S3 download ──────────────────────────────────────────────────────────────

CACHE_FILE = Path("downloads/gold_market_summary.parquet")


def download_market_summary() -> pd.DataFrame:
    if CACHE_FILE.exists():
        print("  (using cached local file)")
        return pd.read_parquet(CACHE_FILE)

    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    proto = "https" if USE_HTTPS else "http"
    s3 = boto3.client(
        "s3",
        endpoint_url=f"{proto}://{ENDPOINT}",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
        config=Config(s3={"addressing_style": "path"}),
    )
    s3.download_file(BUCKET_GOLD, "gold_market_summary.parquet", str(CACHE_FILE))
    return pd.read_parquet(CACHE_FILE)


# ── Plotting helpers ─────────────────────────────────────────────────────────

OUTPUT_DIR = Path("visualizations")
PALETTE = ["#2563eb", "#f59e0b", "#10b981", "#ef4444", "#8b5cf6",
           "#ec4899", "#06b6d4", "#f97316", "#14b8a6", "#6366f1"]


def save(fig, name):
    OUTPUT_DIR.mkdir(exist_ok=True)
    fig.savefig(OUTPUT_DIR / f"{name}.png", dpi=150, bbox_inches="tight",
                facecolor="white")
    plt.close(fig)
    print(f"  Saved {name}.png")


# ── 1. Top 15 Makes by Total Listings ────────────────────────────────────────

def plot_top_makes(df):
    top = (df.groupby("make")["listing_count"].sum()
             .sort_values(ascending=False).head(15))
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(top.index[::-1], top.values[::-1], color=PALETTE[0], edgecolor="white")
    ax.set_xlabel("Total Listings")
    ax.set_title("Top 15 Makes by Listing Volume", fontsize=14, fontweight="bold")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    for bar in bars:
        ax.text(bar.get_width() + top.max() * 0.01, bar.get_y() + bar.get_height() / 2,
                f"{int(bar.get_width()):,}", va="center", fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save(fig, "01_top_makes_by_listings")


# ── 2. Median Price Distribution by Body Type ────────────────────────────────

def plot_price_by_body(df):
    body = (df.groupby("bodytype")
              .agg(median_price=("price_median", "median"),
                   count=("listing_count", "sum"))
              .query("count >= 20")
              .sort_values("median_price", ascending=False))
    fig, ax = plt.subplots(figsize=(10, 6))
    colors = [PALETTE[i % len(PALETTE)] for i in range(len(body))]
    ax.barh(body.index[::-1], body["median_price"].values[::-1], color=colors[::-1])
    ax.set_xlabel("Median Price ($)")
    ax.set_title("Median Listing Price by Body Type", fontsize=14, fontweight="bold")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save(fig, "02_median_price_by_bodytype")


# ── 3. Average Price vs Mileage Scatter (by Make, top 10) ────────────────────

def plot_price_vs_mileage(df):
    top_makes = (df.groupby("make")["listing_count"].sum()
                   .sort_values(ascending=False).head(10).index)
    sub = df[df["make"].isin(top_makes)]
    fig, ax = plt.subplots(figsize=(10, 7))
    for i, make in enumerate(top_makes):
        mk = sub[sub["make"] == make]
        ax.scatter(mk["avg_mileage"], mk["price_mean"], label=make,
                   alpha=0.6, s=mk["listing_count"] * 2, color=PALETTE[i % len(PALETTE)])
    ax.set_xlabel("Average Mileage")
    ax.set_ylabel("Average Price ($)")
    ax.set_title("Average Price vs. Mileage by Make (top 10)", fontsize=14, fontweight="bold")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.legend(fontsize=8, ncol=2, loc="upper right")
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save(fig, "03_price_vs_mileage_scatter")


# ── 4. Model Year Price Trend (top 5 makes) ──────────────────────────────────

def plot_year_price_trend(df):
    top_makes = (df.groupby("make")["listing_count"].sum()
                   .sort_values(ascending=False).head(5).index)
    sub = df[df["make"].isin(top_makes)]
    fig, ax = plt.subplots(figsize=(10, 6))
    for i, make in enumerate(top_makes):
        mk = (sub[sub["make"] == make]
              .groupby("year")["price_mean"].mean()
              .sort_index())
        ax.plot(mk.index, mk.values,
                marker="o", label=make, color=PALETTE[i], linewidth=2, markersize=4)
    ax.set_xlabel("Model Year")
    ax.set_ylabel("Average Price ($)")
    ax.set_title("Average Price by Model Year (Top 5 Makes)", fontsize=14, fontweight="bold")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.legend()
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save(fig, "04_year_price_trend")


# ── 5. Listing Count Heatmap — Make × Year (top 10 makes) ────────────────────

def plot_listing_heatmap(df):
    top_makes = (df.groupby("make")["listing_count"].sum()
                   .sort_values(ascending=False).head(10).index)
    sub = df[df["make"].isin(top_makes)]
    pivot = (sub.groupby(["make", "year"])["listing_count"].sum()
                .unstack(fill_value=0)
                .astype(float))
    # Keep only recent years with data
    recent = [c for c in pivot.columns if c >= 2015]
    pivot = pivot[recent]

    fig, ax = plt.subplots(figsize=(12, 6))
    im = ax.imshow(pivot.values, aspect="auto", cmap="YlOrRd")
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns.astype(int), rotation=45, fontsize=9)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index, fontsize=9)
    ax.set_title("Listing Volume Heatmap — Make × Model Year", fontsize=14, fontweight="bold")
    fig.colorbar(im, ax=ax, label="Listings", shrink=0.8)
    fig.tight_layout()
    save(fig, "05_listing_heatmap")


# ── 6. Price Variability (Coefficient of Variation) by Make ───────────────────

def plot_price_variability(df):
    cv = (df.groupby("make")
            .apply(lambda g: pd.Series({
                "weighted_cv": np.average(g["price_cv_pct"].dropna(),
                                          weights=g.loc[g["price_cv_pct"].notna(), "listing_count"])
                                          if g["price_cv_pct"].notna().any() else np.nan,
                "total_listings": g["listing_count"].sum(),
            }))
            .dropna()
            .query("total_listings >= 50")
            .sort_values("weighted_cv", ascending=False)
            .head(15))
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(cv.index[::-1], cv["weighted_cv"].values[::-1], color=PALETTE[3])
    ax.set_xlabel("Coefficient of Variation (%)")
    ax.set_title("Price Variability by Make (CV %)", fontsize=14, fontweight="bold")
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save(fig, "06_price_variability_by_make")


# ── 7. Average MPG (Combined) by Body Type ───────────────────────────────────

def plot_mpg_by_body(df):
    mpg = (df.groupby("bodytype")
             .agg(avg_mpg=("avg_mpg_combined", "mean"),
                  count=("listing_count", "sum"))
             .query("count >= 20 and avg_mpg > 0")
             .sort_values("avg_mpg", ascending=True))
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(mpg.index, mpg["avg_mpg"], color=PALETTE[2])
    ax.set_xlabel("Average Combined MPG")
    ax.set_title("Fuel Efficiency by Body Type", fontsize=14, fontweight="bold")
    ax.spines[["top", "right"]].set_visible(False)
    for i, (_, row) in enumerate(mpg.iterrows()):
        ax.text(row["avg_mpg"] + 0.3, i, f"{row['avg_mpg']:.1f}", va="center", fontsize=9)
    fig.tight_layout()
    save(fig, "07_mpg_by_bodytype")


# ── 8. Average Discount % — Top 15 Makes ─────────────────────────────────────

def plot_discount_by_make(df):
    disc = (df.groupby("make")
              .agg(avg_disc=("avg_discount_pct", "mean"),
                   count=("listing_count", "sum"))
              .query("count >= 50")
              .sort_values("avg_disc", ascending=False)
              .head(15))
    fig, ax = plt.subplots(figsize=(10, 6))
    colors = [PALETTE[0] if v >= 0 else PALETTE[3] for v in disc["avg_disc"].values[::-1]]
    ax.barh(disc.index[::-1], disc["avg_disc"].values[::-1], color=colors)
    ax.set_xlabel("Average Discount from List Price (%)")
    ax.set_title("Average Discount % by Make", fontsize=14, fontweight="bold")
    ax.axvline(0, color="gray", linewidth=0.8, linestyle="--")
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save(fig, "08_discount_by_make")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("Downloading gold_market_summary from lakegold...")
    df = download_market_summary()
    print(f"Loaded {len(df):,} rows, {len(df.columns)} columns")
    print(f"Columns: {list(df.columns)}")
    print(f"Makes: {df['make'].nunique()}, Years: {df['year'].min()}-{df['year'].max()}")
    print()

    print("Generating visualizations...")
    plot_top_makes(df)
    plot_price_by_body(df)
    plot_price_vs_mileage(df)
    plot_year_price_trend(df)
    plot_listing_heatmap(df)
    plot_price_variability(df)
    plot_mpg_by_body(df)
    plot_discount_by_make(df)

    print(f"\nAll plots saved to {OUTPUT_DIR.resolve()}/")


if __name__ == "__main__":
    main()
