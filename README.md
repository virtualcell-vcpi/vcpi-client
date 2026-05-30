# vcpi — Virtual Cell Pharmacology Initiative Python Client

A Python client for accessing VCPI sequencing datasets, compound chemistry, and experimental metadata. Built on [Polars](https://pola.rs) and [DuckDB](https://duckdb.org), it lets you run SQL directly against multi-experiment parquet files on S3 without downloading anything, or pull full datasets locally when you need them.

---

## Installation
```
pip install git+https://github.com/virtualcell-vcpi/vcpi-client.git
```

To upgrade to the latest version:
```
pip install --upgrade git+https://github.com/virtualcell-vcpi/vcpi-client.git
```

**Requirements:** Python ≥ 3.10

---

## Authentication

### Getting a token

Generate your personal access token at **[thevirtualcell.com/dashboard](https://thevirtualcell.com/dashboard)** — click the Settings icon to find your token.

### Setting your token

**Option 1 — environment variable (recommended)**
```bash
export TVC_TOKEN="your-token-here"
```

Add this to your `.bashrc`, `.zshrc`, or shell profile so it's set automatically on every session.

**Option 2 — interactive login (stores in system keyring)**
```python
import vcpi
vcpi.login()
```

---

## Quick Start
```python
import vcpi

# See what datasets you have access to
datasets = vcpi.list_datasets()

# Query metadata and chemistry without downloading anything
df = vcpi.query(
    job="vcpi-0001",
    sql="SELECT * FROM metadata WHERE percent_mitochondrial < 20 LIMIT 5"
)

# Download a single experiment
exp = vcpi.load_experiment("vcpi-0001")
exp["data"]      # gene expression matrix (genes × samples)
exp["metadata"]  # sample metadata
exp["chemistry"] # compound chemistry

# Download and merge multiple experiments — expressed genes only
combined = vcpi.load_experiments(["vcpi-0001", "vcpi-0002"])

# Filter to specific samples before merging
combined = vcpi.load_experiments(
    ["vcpi-0001", "vcpi-0002"],
    sql="SELECT * FROM metadata WHERE user_compound_id = 'DMSO'"
)
```

---

## Core Concepts

### Three ways to work with data

| Approach | Function | When to use |
| --- | --- | --- |
| **Query** | `query()` | Filter and explore metadata and chemistry — no download, no parquet |
| **Download (single)** | `load_experiment()` | Full gene expression matrix for one experiment, all genes including zeros |
| **Download (multi)** | `load_experiments()` | Multiple experiments merged, expressed genes only, optional sample filter |

`query()` runs SQL against `metadata` and `chemistry` as in-memory DuckDB tables. The parquet is never touched. Use it to explore and filter before downloading.

`load_experiment()` downloads the full gene expression matrix for a single experiment. All genes are returned, including zero-expression rows.

`load_experiments()` downloads and merges multiple experiments. Zero-expression genes are removed before joining. An optional `sql` argument (same syntax as `query()`) filters samples before merging. If combined data exceeds 8 GB, individual frames are returned as a dict instead of a joined matrix — check `result["fallback"]` to detect this. A warning is emitted above 2 GB.

---

## API Reference

### `login()`

Validate and store your API key in the system keychain. Accepts an optional token argument; if omitted, prompts interactively.
```python
vcpi.login()            # interactive
vcpi.login("my-token")  # direct
```

---

### `list_datasets()`

Returns a Polars DataFrame of all datasets the authenticated user can access.
```python
datasets = vcpi.list_datasets()
datasets[["job_id", "job_name"]]
```

---

### `query()`

Run SQL against `metadata` and `chemistry`. Pass `job` to scope to one experiment, or omit it to query across all accessible datasets. `job` accepts either a job ID or human-readable name. `sql` defaults to `SELECT * FROM metadata LIMIT 10`.

**Available tables:** `metadata`, `chemistry`
```python
# Single experiment
df = vcpi.query(
    job="vcpi-0001",
    sql="SELECT * FROM metadata LIMIT 10"
)

# Filter by QC
df = vcpi.query(
    job="vcpi-0001",
    sql="""
        SELECT sequenced_id, user_compound_id, percent_mitochondrial
        FROM metadata
        WHERE percent_mitochondrial < 20
    """
)

# Join metadata and chemistry
df = vcpi.query(
    job="vcpi-0001",
    sql="""
        SELECT m.sequenced_id, m.user_compound_id,
               c.smiles, c.log_p, c.molecular_weight
        FROM metadata m
        JOIN chemistry c ON c.compound = m.compound
    """
)

# Cross-experiment: DMSO controls across all jobs
df = vcpi.query(
    sql="SELECT * FROM metadata WHERE user_compound_id = 'DMSO'"
)

# Cross-experiment: compounds tested in multiple jobs
df = vcpi.query(
    sql="""
        SELECT user_compound_id,
               COUNT(DISTINCT job_id) AS n_experiments
        FROM metadata
        GROUP BY user_compound_id
        HAVING COUNT(DISTINCT job_id) > 1
    """
)
```

---

### `describe()`

Returns a dict with the schema of the `metadata` and `chemistry` tables. Pass `job` to scope to one experiment, or omit to describe across all experiments.
```python
schemas = vcpi.describe("vcpi-0001")
schemas["metadata"]["column_name"].to_list()
schemas["chemistry"]["column_name"].to_list()
```

---

### `load_dataset()`

Download the raw sequencing parquet only. Prefer `load_experiment()` for most use cases.
```python
df = vcpi.load_dataset("vcpi-0001")
```

---

### `load_metadata()` / `load_chem()`
```python
meta = vcpi.load_metadata("vcpi-0001")
chem = vcpi.load_chem("vcpi-0001")
```

---

### `load_experiment()`

Download sequencing data, metadata, and chemistry for a single experiment. Returns all genes including zero-expression rows.
```python
exp = vcpi.load_experiment("vcpi-0001")

exp["data"]      # full gene expression matrix
exp["metadata"]  # sample metadata
exp["chemistry"] # compound chemistry
exp["job_id"]    # resolved job ID
```

---

### `load_experiments()`

Download and merge multiple experiments. Zero-expression genes are removed before joining. Pass `sql` to filter samples before merging, using the same syntax as `query()`. Omit `sql` to include all samples.

If combined data exceeds 8 GB, a warning is raised and individual frames are returned as a dict keyed by job ID rather than a joined matrix — check `result["fallback"]` to detect this.
```python
# Basic merge
combined = vcpi.load_experiments(["vcpi-0001", "vcpi-0002"])

# With sample filter
combined = vcpi.load_experiments(
    ["vcpi-0001", "vcpi-0002"],
    sql="SELECT * FROM metadata WHERE user_compound_id = 'DMSO'"
)

combined["data"]      # joined expression matrix, expressed genes only
combined["metadata"]  # concatenated metadata
combined["chemistry"] # concatenated chemistry
combined["job_ids"]   # list of resolved job IDs
combined["fallback"]  # True if memory limit hit — data is a dict keyed by job_id

# Handle fallback
if combined["fallback"]:
    for jid in combined["job_ids"]:
        print(jid, combined["data"][jid].shape)
else:
    print(combined["data"].shape)
```

---

## Using from R (via reticulate)
```r
library(reticulate)
library(arrow)

use_condaenv("r-reticulate", required = TRUE)
vcpi <- import("vcpi")

Sys.setenv(TVC_TOKEN = "your-token-here")

# Helper: Polars -> R data.frame via Arrow (no pandas required)
polars_to_r <- function(df) {
  as.data.frame(arrow::as_arrow_table(df$to_arrow()))
}

# List datasets
datasets <- polars_to_r(vcpi$list_datasets())

# Query metadata
df <- polars_to_r(vcpi$query(
  job = "vcpi-0001",
  sql = "SELECT * FROM metadata LIMIT 100"
))

# DMSO controls across all experiments
controls <- polars_to_r(vcpi$query(
  sql = "SELECT * FROM metadata WHERE user_compound_id = 'DMSO'"
))

# Single experiment
exp  <- vcpi$load_experiment("vcpi-0001")
seq  <- polars_to_r(exp[["data"]])
meta <- polars_to_r(exp[["metadata"]])
chem <- polars_to_r(exp[["chemistry"]])

# Multiple experiments merged
combined <- vcpi$load_experiments(list("vcpi-0001", "vcpi-0002"))

if (!combined[["fallback"]]) {
  data <- polars_to_r(combined[["data"]])
} else {
  for (jid in combined[["job_ids"]]) {
    cat(jid, ":", combined[["data"]][[jid]]$shape, "\n")
  }
}
```

> **Note:** Use `arrow::as_arrow_table(df$to_arrow())` for conversion — pandas is not a declared dependency of the vcpi client.

---

## Data Model

### Sequencing (`exp["data"]`)

Wide-format gene expression matrix. Rows are Ensembl gene IDs (GENCODE v48), columns are sample IDs (`sequenced_id`).
```
shape: (~60,000 genes × ~N samples)

┌─────────────────┬───────────┬───────────┬─────┐
│ gene_id         │ 101160268 │ 101160269 │ … │
╞═════════════════╪═══════════╪═══════════╪═════╡
│ ENSG00000223972 │ 0.0       │ 12.4      │ … │
│ ENSG00000227232 │ 3.1       │ 0.0       │ … │
└─────────────────┴───────────┴───────────┴─────┘
```

### Metadata

One row per sample. Key columns: `sequenced_id`, `job_id`, `compound`, `user_compound_id`, `compound_concentration`, `compound_concentration_unit`, `cell_line`, `timepoint`, `is_control`, `total_sequenced_reads`, `percent_mitochondrial`, `percent_mapped`, `percent_duplicated`, `ngenes3`.

### Chemistry

One row per compound. Key columns: `compound`, `user_compound_id`, `smiles`, `purity_pct`, `molecular_weight`, `log_p`, `tpsa`, `inchi_key`, `num_rotatable_bonds`, `num_h_acceptors`, `num_h_donors`, `num_atoms`, `num_bonds`.

### How the three tables relate
```
sequencing (wide)            metadata                chemistry
─────────────────────        ────────────────────    ──────────────────
gene_id | 101160268 | …  ←── sequenced_id           compound ──→ smiles
                             compound ──────────────→
                             job_id, cell_line, …
```

---

## Experimental Methods

### Design

384-well format. THP-1 cells seeded at 20,000 cells/well, 24h then treated for 24h. Library compounds tested in technical duplicates at six concentrations [0.03, 0.1, 0.3, 1, 3, 10 µM]. DMSO inert control at 0.15% in 8 replicates/plate. Staurosporine cell death control at 10 µM (4 replicates/plate). Three transcriptional controls (Brefeldin A, Rigosertib, Trichostatin A) at 10 µM (4 replicates/plate).

### Feature Counts

Gene-level UMI counts obtained via rRNA removal (bbduk), STAR alignment against hg38, deduplication (umi-tools), and counting (featureCounts). Gene IDs from GENCODE v48.

---

## Example EDA Workflows

The repository includes a control-drug PCA workflow for local QC analysis:

```bash
python examples/control_drug_pca.py \
  --experiment tvc-bhr-009:/path/to/metadata-tvc-bhr-009.csv:/path/to/vcpi_tvc-bhr-009_counts.parquet \
  --experiment tvc-kdl-010:/path/to/metadata-tvc-kdl-010.csv:/path/to/vcpi_tvc-kdl-010_counts.parquet \
  --experiment tvc-qnu-012:/path/to/metadata-tvc-qnu-012.csv:/path/to/vcpi_tvc-qnu-012_counts.parquet \
  --out-dir eda_outputs/full_control_drug_pcas
```

See [`docs/control_drug_pca_eda.md`](docs/control_drug_pca_eda.md) for an example interpretation of DMSO,
Staurosporin, Brefeldin-A, Trichostatin-A, and Rigosertib control PCAs. See
[`docs/drug_seq_project_notes.md`](docs/drug_seq_project_notes.md) for a team-facing summary of the dataset,
QC findings, and modeling recommendations. See [`docs/qnu_eval_split_pca.md`](docs/qnu_eval_split_pca.md)
for a PCA of the selected `tvc-qnu-012` eval-like plates from the ridge-sweep validation setup. See
[`docs/submission_prediction_pca.md`](docs/submission_prediction_pca.md) for a PCA of the
`submission_qnu_rdkit_pca128_ridge_alpha1000.parquet` model predictions. See
[`docs/train_test_expression_pca.md`](docs/train_test_expression_pca.md) for a PCA comparing observed train
compound expression with predicted test compound expression.

---

## Validating every release end-to-end

The package ships with a `test-vcpi-releases` console script that downloads every release the current `TVC_TOKEN` can access, checks that the sample IDs in the metadata table exactly match the sample columns in the wide-format parquet, and writes one [AnnData](https://anndata.readthedocs.io/) (`.h5ad`) per release.

It depends on `anndata`, `numpy`, and `pandas`, so install with the `validate` extras:

```bash
pip install "vcpi[validate]"
# or, from a clone:
pip install -e ".[validate]"
```

Then:

```bash
# Validate every accessible release and write .h5ad files to ./vcpi-anndata
test-vcpi-releases

# Pick a specific output directory
test-vcpi-releases --output-dir /path/to/out

# Validate only — don't write .h5ad files
test-vcpi-releases --no-write

# Limit to specific releases (repeat --job for multiple)
test-vcpi-releases --job vcpi-0001 --job vcpi-0002
```

Exit codes: `0` if every release is clean, `1` if any release had warnings (e.g. samples in metadata that aren't in the parquet, or vice versa), `2` if any release failed outright.

---

## Troubleshooting

**`PermissionError: TVC_TOKEN not found`** — Run `export TVC_TOKEN="your-token"` or call `vcpi.login()`.

**`401 Unauthorized`** — Token invalid or expired. Generate a new one at [thevirtualcell.com/dashboard](https://thevirtualcell.com/dashboard).

**`load_experiment()` is slow** — The expression matrix is large (~400 MB). Typical download time is 20–60 seconds on a good connection.

**`load_experiments()` returns `fallback: True`** — Combined data exceeded the 8 GB join limit. Individual frames are available via `result["data"][job_id]`.

---

## Dependencies

| Package | Purpose |
| --- | --- |
| `polars` | DataFrame engine |
| `duckdb` | SQL engine with S3/parquet range-request support |
| `httpx` | HTTP client |
| `keyring` | Secure token storage |
| `tqdm` | Download progress bar |
| `pyarrow` | Arrow serialization |

---

## License

MIT © Ginkgo Datapoints
