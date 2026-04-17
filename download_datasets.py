#!/usr/bin/env python3
"""
TEG Benchmark Dataset Downloader
=================================
Downloads the TEG-DB datasets from HuggingFace into a local directory.
Only the processed PKL files are fetched; pre-computed embedding files
(*.pt) are skipped to reduce download size.

Usage
-----
  python download_datasets.py
  python download_datasets.py --output_dir ./Dataset
"""

import argparse
from huggingface_hub import snapshot_download


def parse_args():
    p = argparse.ArgumentParser(
        description="Download TEG-DB datasets from HuggingFace."
    )
    p.add_argument(
        "--output_dir", default="./Dataset",
        help="Local directory to download into (default: ./Dataset)",
    )
    return p.parse_args()


def main():
    args = parse_args()

    print(f"Downloading TEG-DB datasets to {args.output_dir} ...")
    snapshot_download(
        repo_id="ZhuofengLi/TEG-Datasets",
        repo_type="dataset",
        local_dir=args.output_dir,
        ignore_patterns="*.pt",
    )
    print(f"\nDone. Datasets saved to {args.output_dir}")
    print(f"\nNext step — convert to Neptune CSV:")
    print(f"  python pkl_to_csv.py --dataset_dir {args.output_dir} --output_dir ./neptune_csv")


if __name__ == "__main__":
    main()
