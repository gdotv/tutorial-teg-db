#!/usr/bin/env python3
"""
TEG Benchmark PKL to Neptune-compatible CSV Converter
======================================================
Reads PyTorch Geometric Data objects (*.pkl) from the TEG benchmark and writes
two CSV files per dataset in Amazon Neptune Bulk Loader format, ready to upload
to S3 and ingest into Neptune via the bulk-load API.

Neptune Bulk Loader format
--------------------------
  Vertex file  ~id, ~label, text:String, teg_label:String
  Edge file    ~id, ~from, ~to, ~label, text:String [, score:Double]

After uploading {dataset}_nodes.csv and {dataset}_edges.csv to an S3 bucket,
trigger Neptune Bulk Loader:

    POST https://{neptune-endpoint}:8182/loader
    {
      "source":     "s3://your-bucket/neptune-csv/reddit/",
      "format":     "csv",
      "iamRoleArn": "arn:aws:iam::ACCOUNT:role/NeptuneLoadFromS3",
      "region":     "us-east-1"
    }

Usage
-----
  # Convert all available datasets
  python pkl_to_csv.py --dataset_dir ./Dataset --output_dir ./neptune_csv

  # Convert a specific dataset
  python pkl_to_csv.py --dataset_dir ./Dataset --output_dir ./neptune_csv --only reddit

  # List known datasets
  python pkl_to_csv.py --list

Dataset quirks handled automatically
--------------------------------------
  Reddit     text_nodes is stored in edge-iteration order, not node-id order.
             The nodetotext mapping is reconstructed from edge_index.
  Amazon/*   The label attribute is named text_node_labels (not node_labels).
             edge_score carries per-edge star ratings (1–5, stored as float).
  Goodreads  Same quirks as Amazon. edge_score is a torch.long tensor.
  Twitter    text_nodes is correctly indexed. Two edge kinds are present:
             POSTED (authortotweet) and MENTIONED_IN (mentioned_usertotweet).
  Arxiv      Homogeneous citation graph; both endpoints carry the Paper label.
"""

import argparse
import csv
import pickle
import sys
from pathlib import Path

DATASETS = {
    "reddit": {
        "pkl":          "reddit/processed/reddit.pkl",
        "label_attr":   "node_labels",
        "has_score":    False,
        "text_indexed": False,   # edge-order layout — special handling
        "node_types":   ("User", "Subreddit"),
        "edge_type":    "POSTED",
    },
    "twitter": {
        "pkl":          "twitter/processed/twitter.pkl",
        "label_attr":   "node_labels",
        "has_score":    False,
        "text_indexed": True,
        "node_types":   ("User", "Tweet"),
        "edge_type":    "POSTED",
    },
    "arxiv": {
        "pkl":          "arxiv/processed/arxiv.pkl",
        "label_attr":   "node_labels",
        "has_score":    False,
        "text_indexed": True,
        "node_types":   ("Paper", "Paper"),
        "edge_type":    "CITES",
    },
    "amazon_apps": {
        "pkl":          "amazon_apps/processed/apps.pkl",
        "label_attr":   "text_node_labels",
        "has_score":    True,
        "text_indexed": True,
        "node_types":   ("Reviewer", "Product"),
        "edge_type":    "REVIEWED",
    },
    "amazon_baby": {
        "pkl":          "amazon_baby/processed/baby.pkl",
        "label_attr":   "text_node_labels",
        "has_score":    True,
        "text_indexed": True,
        "node_types":   ("Reviewer", "Product"),
        "edge_type":    "REVIEWED",
    },
    "amazon_movie": {
        "pkl":          "amazon_movie/processed/movie.pkl",
        "label_attr":   "text_node_labels",
        "has_score":    True,
        "text_indexed": True,
        "node_types":   ("Reviewer", "Product"),
        "edge_type":    "REVIEWED",
    },
    "goodreads_children": {
        "pkl":          "goodreads_children/processed/children.pkl",
        "label_attr":   "text_node_labels",
        "has_score":    True,
        "text_indexed": True,
        "node_types":   ("User", "Book"),
        "edge_type":    "REVIEWED",
    },
    "goodreads_comics": {
        "pkl":          "goodreads_comics/processed/comics.pkl",
        "label_attr":   "text_node_labels",
        "has_score":    True,
        "text_indexed": True,
        "node_types":   ("User", "Book"),
        "edge_type":    "REVIEWED",
    },
    "goodreads_crime": {
        "pkl":          "goodreads_crime/processed/crime.pkl",
        "label_attr":   "text_node_labels",
        "has_score":    True,
        "text_indexed": True,
        "node_types":   ("User", "Book"),
        "edge_type":    "REVIEWED",
    },
    "goodreads_history": {
        "pkl":          "goodreads_history/processed/history.pkl",
        "label_attr":   "text_node_labels",
        "has_score":    True,
        "text_indexed": True,
        "node_types":   ("User", "Book"),
        "edge_type":    "REVIEWED",
    },
}

def safe_str(value) -> str:
    """Flatten any Python or PyTorch value to a plain string for CSV output."""
    if value is None:
        return ""
    if isinstance(value, list):
        return "|".join(str(v) for v in value)
    if hasattr(value, "item"):        
        return str(value.item())
    return str(value)


def to_scalar(value):
    """Unwrap a torch scalar tensor to a Python primitive."""
    if hasattr(value, "item"):
        return value.item()
    return value


def infer_node_type(text: str, src_type: str, dst_type: str) -> str:
    """
    Derive the vertex label from a node's text content.

    Each TEG dataset encodes node identity in the text field:
      Reddit    : "subreddit <name>" to Subreddit | "user <id> ..." to User
      Twitter   : "tweet"            to Tweet     | "user" / "mentioned user" to User
      Amazon    : "reviewer"         to Reviewer  | product description to Product
      Goodreads : "user"             to User      | book description to Book
      Arxiv     : any text           to Paper     (homogeneous)
    """
    if src_type == dst_type:
        return src_type

    t = (text or "").lower().strip()

    if t.startswith("subreddit"):
        return "Subreddit"
    if t.startswith("user") or t.startswith("mentioned user"):
        return "User"
    if t == "tweet":
        return "Tweet"
    if t == "reviewer":
        return "Reviewer"
    if t in ("item", ""):
        return dst_type     

    return dst_type         


def load_pkl(path: Path):
    with open(path, "rb") as fh:
        return pickle.load(fh)



def build_node_map_indexed(data, label_attr: str):
    """
    Standard case: text_nodes[i] is the canonical text for node i.
    Applies to Twitter, Arxiv, Amazon, and Goodreads.
    """
    raw_labels = getattr(data, label_attr, None)
    node_text  = {i: safe_str(t) for i, t in enumerate(data.text_nodes)}
    node_label = {}
    if raw_labels is not None:
        for i, lbl in enumerate(raw_labels):
            node_label[i] = safe_str(lbl)
    return node_text, node_label


def build_node_map_reddit(data):
    """
    Reddit anomaly: the processing loop appends node texts in edge-iteration
    order rather than indexing by node id.  Two entries are written per edge:

        text_nodes[2*i]   to text for edge_index[1][i]  (subreddit / dst)
        text_nodes[2*i+1] to text for edge_index[0][i]  (user / src)

    This function reconstructs the correct node_id to text mapping by walking
    edge_index alongside text_nodes.
    """
    edge_index  = data.edge_index
    text_nodes  = data.text_nodes
    node_labels = data.node_labels

    node_text  = {}
    node_label = {}

    for i in range(edge_index.shape[1]):
        src_id = to_scalar(edge_index[0][i])
        dst_id = to_scalar(edge_index[1][i])

        if dst_id not in node_text:
            node_text[dst_id]  = safe_str(text_nodes[2 * i])
            node_label[dst_id] = safe_str(node_labels[2 * i])

        if src_id not in node_text:
            node_text[src_id]  = safe_str(text_nodes[2 * i + 1])
            node_label[src_id] = safe_str(node_labels[2 * i + 1])

    return node_text, node_label



def export_dataset(name: str, cfg: dict, dataset_dir: Path, output_dir: Path) -> bool:
    pkl_path = dataset_dir / cfg["pkl"]
    if not pkl_path.exists():
        print(f"  [SKIP] {name}: PKL not found at {pkl_path}")
        print(f"         (dataset may need git lfs pull from the HuggingFace repo)")
        return False

    out_dir = output_dir / name
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"  Loading {pkl_path.name}  ({pkl_path.stat().st_size / 1_048_576:.1f} MB) …")
    data = load_pkl(pkl_path)

    edge_index = data.edge_index    # torch.Tensor [2, E]
    text_edges = data.text_edges    # List[str]
    num_edges  = edge_index.shape[1]

    # Edge scores (Amazon / Goodreads only)
    edge_scores = None
    if cfg["has_score"] and hasattr(data, "edge_score"):
        edge_scores = data.edge_score

    # Build node text/label maps
    if not cfg["text_indexed"]:
        node_text, node_label = build_node_map_reddit(data)
    else:
        node_text, node_label = build_node_map_indexed(data, cfg["label_attr"])

    src_type, dst_type = cfg["node_types"]
    edge_type          = cfg["edge_type"]
    num_nodes          = len(node_text)

    print(f"  Nodes: {num_nodes:,}   Edges: {num_edges:,}")


    nodes_file = out_dir / f"{name}_nodes.csv"
    with open(nodes_file, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, quoting=csv.QUOTE_ALL)
        writer.writerow(["~id", "~label", "text:String", "teg_label:String"])
        for node_id in sorted(node_text.keys()):
            text  = node_text.get(node_id, "")
            label = node_label.get(node_id, "")
            ntype = infer_node_type(text, src_type, dst_type)
            writer.writerow([f"{name}_{node_id}", ntype, text, label])

    print(f"  to {nodes_file}")

   
    edges_file = out_dir / f"{name}_edges.csv"
    with open(edges_file, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, quoting=csv.QUOTE_ALL)
        header = ["~id", "~from", "~to", "~label", "text:String"]
        if edge_scores is not None:
            header.append("score:Double")
        writer.writerow(header)

        for i in range(num_edges):
            src   = to_scalar(edge_index[0][i])
            dst   = to_scalar(edge_index[1][i])
            etext = safe_str(text_edges[i]) if i < len(text_edges) else ""
            row   = [f"{name}_{i}", f"{name}_{src}", f"{name}_{dst}", edge_type, etext]
            if edge_scores is not None:
                row.append(to_scalar(edge_scores[i]))
            writer.writerow(row)

    print(f"  to {edges_file}")
    return True



def parse_args():
    p = argparse.ArgumentParser(
        description="Convert TEG benchmark PKL files to Neptune-compatible CSV."
    )
    p.add_argument("--dataset_dir", default="./Dataset",
                   help="Root of the TEG Dataset directory (default: ./Dataset)")
    p.add_argument("--output_dir", default="./neptune_csv",
                   help="Output root directory (default: ./neptune_csv)")
    p.add_argument("--only", metavar="NAME", nargs="+",
                   help="Convert only the named dataset(s). Use --list for names.")
    p.add_argument("--list", action="store_true",
                   help="Print all known dataset names and their PKL paths, then exit.")
    return p.parse_args()


def main():
    args = parse_args()

    if args.list:
        print("Known datasets:")
        for name, cfg in DATASETS.items():
            print(f"  {name:<25}  {cfg['pkl']}")
        return

    dataset_dir = Path(args.dataset_dir).expanduser().resolve()
    output_dir  = Path(args.output_dir).expanduser().resolve()

    if not dataset_dir.exists():
        sys.exit(f"Error: dataset_dir '{dataset_dir}' does not exist.")

    target_names = args.only if args.only else list(DATASETS.keys())
    unknown = [n for n in target_names if n not in DATASETS]
    if unknown:
        sys.exit(f"Unknown dataset(s): {unknown}. Use --list to see valid names.")

    output_dir.mkdir(parents=True, exist_ok=True)
    ok = skipped = 0

    for name in target_names:
        print(f"\n[{name}]")
        if export_dataset(name, DATASETS[name], dataset_dir, output_dir):
            ok += 1
        else:
            skipped += 1

    print(f"\nDone. {ok} converted, {skipped} skipped.")
    if skipped:
        print("Skipped datasets have PKL files managed by HuggingFace LFS.")
    print(f"\nNext step — upload to S3:")
    print(f"  aws s3 sync {output_dir} s3://your-bucket/neptune-csv/")


if __name__ == "__main__":
    main()
