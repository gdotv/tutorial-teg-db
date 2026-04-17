# TEG-DB to Neptune Property Graph Pipeline

A conversion pipeline that takes the [TEG-DB](https://huggingface.co/datasets/ZhuofengLi/TEG-Datasets) (Text-Enriched Graph) benchmark datasets from their PyTorch Geometric PKL serialization format and produces Amazon Neptune-compatible CSV files ready for bulk loading.

This repository accompanies the blog series *Building a Property Graph Pipeline from the TEG Benchmark*.

## Datasets

The pipeline covers all ten TEG-DB datasets across four domains:

| Dataset | Domain | Vertices | Edges | Edge Label |
|---|---|---:|---:|---|
| Arxiv | Academic / Citation | 169,343 | 1,196,640 | CITES |
| Reddit | Social Network | 128,937 | 512,621 | POSTED |
| Twitter | Social Network | 60,785 | 77,926 | POSTED |
| Amazon Apps | E-Commerce | 100,480 | 753,122 | REVIEWED |
| Amazon Baby | E-Commerce | 186,793 | 1,241,083 | REVIEWED |
| Amazon Movie | E-Commerce | 267,010 | 1,697,543 | REVIEWED |
| Goodreads Children | Literary Rec. | 344,931 | 1,417,940 | REVIEWED |
| Goodreads Comics | Literary Rec. | 217,992 | 1,194,148 | REVIEWED |
| Goodreads Crime | Literary Rec. | 838,665 | 4,676,511 | REVIEWED |
| Goodreads History | Literary Rec. | 1,020,266 | 5,344,650 | REVIEWED |

## Prerequisites

- Python 3.10+
- PyTorch and PyTorch Geometric (for PKL deserialization)
- `huggingface_hub`

```bash
pip install torch torch_geometric huggingface_hub
```

## Usage

### 1. Download datasets from HuggingFace

```bash
python download_datasets.py
```

This fetches all TEG-DB datasets into `./Dataset`, skipping pre-computed embedding files (`*.pt`) to reduce download size. Use `--output_dir` to change the destination.

### 2. Convert PKL files to Neptune CSV

```bash
python pkl_to_csv.py --dataset_dir ./Dataset --output_dir ./neptune_csv
```

This produces two CSV files per dataset in Neptune Bulk Loader format:

- `{dataset}_nodes.csv` with headers `~id, ~label, text:String, teg_label:String`
- `{dataset}_edges.csv` with headers `~id, ~from, ~to, ~label, text:String [, score:Double]`

To convert a single dataset:

```bash
python pkl_to_csv.py --only arxiv
```

To list all known dataset names:

```bash
python pkl_to_csv.py --list
```

### 3. Upload to S3 and bulk load into Neptune

```bash
aws s3 sync ./neptune_csv s3://your-bucket/neptune-csv/
```

Then trigger the Neptune Bulk Loader from within the VPC (see the blog post for the full walkthrough):

```bash
curl -X POST \
  -H 'Content-Type: application/json' \
  https://<NEPTUNE_ENDPOINT>:8182/loader \
  -d '{
    "source": "s3://your-bucket/neptune-csv/arxiv/",
    "format": "csv",
    "iamRoleArn": "arn:aws:iam::<ACCOUNT_ID>:role/NeptuneLoadFromS3",
    "region": "eu-west-2",
    "failOnError": "FALSE",
    "parallelism": "MEDIUM",
    "queueRequest": "TRUE"
  }'
```

## Output Format

Neptune distinguishes node and edge files by their CSV headers:

**Nodes** require `~id` and `~label`:
```
~id,~label,text:String,teg_label:String
arxiv_0,Paper,"The paper titled...",arxiv cs cr
```

**Edges** require `~id`, `~from`, `~to`, and `~label`:
```
~id,~from,~to,~label,text:String
arxiv_0,arxiv_104447,arxiv_13091,CITES,"The full results of this study..."
```

## Dataset Quirks

The conversion script handles several inconsistencies in the TEG-DB serialization:

- **Reddit**: `text_nodes` is stored in edge-iteration order, not node-id order. The node-to-text mapping is reconstructed from `edge_index`.
- **Amazon / Goodreads**: The label attribute is named `text_node_labels` rather than `node_labels`. `edge_score` carries per-edge star ratings.
- **Twitter**: Two node types (`User`, `Tweet`) are inferred from the text content.
- **Arxiv**: Homogeneous citation graph where both endpoints are `Paper` vertices.

## References

- Li, Z. et al. (2024). *TEG-DB: A Comprehensive Dataset and Benchmark of Textual-Edge Graphs.* NeurIPS 2024 Datasets and Benchmarks Track.
- [TEG-DB on HuggingFace](https://huggingface.co/datasets/ZhuofengLi/TEG-Datasets)
- [Amazon Neptune Bulk Loader documentation](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load.html)

## License

The TEG-DB datasets are subject to the licensing terms of their original sources (OGB, Amazon Reviews, Goodreads, Reddit, Twitter). This pipeline code is provided as a companion to the blog series.
