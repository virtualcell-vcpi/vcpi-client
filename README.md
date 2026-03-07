# vcpi — Virtual Cell Pharmacology Initiative Python Client

A Python client for accessing VCPI sequencing datasets, compound chemistry, and experimental metadata. Built on [Polars](https://pola.rs) and [DuckDB](https://duckdb.org), it lets you run SQL directly against multi-experiment parquet files on S3 without downloading anything, or pull full datasets locally when you need them.

---

## Installation

```bash
pip install git+https://github.com/GinkgoDatapoints/vcpi-package.git
```

To install a specific version or branch:

```bash
# Specific release tag
pip install git+https://github.com/GinkgoDatapoints/vcpi-package.git@v0.1.0

# Specific branch
pip install git+https://github.com/GinkgoDatapoints/vcpi-package.git@main
```

To upgrade to the latest version:

```bash
pip install --upgrade git+https://github.com/GinkgoDatapoints/vcpi-package.git
```

**Requirements:** Python ≥ 3.9

---

## Authentication

### Getting a token

Generate your personal access token at **[thevirtualcell.com](https://thevirtualcell.com)**. Once logged in, navigate to your account settings to create one.

### Setting your token

**Option 1 — environment variable (recommended)**

```bash
export TVC_TOKEN="your-token-here"
```

Add this to your `.bashrc`, `.zshrc`, or shell profile so it's set automatically on every session.

**Option 2 — system keyring**

```python
import keyring
keyring.set_password("vcpi-client", "TVC_TOKEN", "your-token-here")
```

Once stored, `import vcpi` will pick it up automatically with no further configuration.

---

## Data Access

### Dataset availability

New datasets are added on a rolling basis as experiments are completed. Run `vcpi.list_datasets()` at any time to see what's currently available to you.

### Superuser early access

Superusers receive **6 months of early access** to new datasets before they are released to the general user base. Becoming a superuser is straightforward — visit **[thevirtualcell.com](https://thevirtualcell.com)** for details on how to apply.

Early access datasets appear in `list_datasets()` automatically once your superuser status is active — no extra configuration needed.

---

## Quick Start

```python
import vcpi

# 1. See what datasets you have access to
datasets = vcpi.list_datasets()
print(datasets)

# 2. Query metadata and chemistry without downloading anything
df = vcpi.query(
    job_id="your-job-id",
    sql="SELECT * FROM metadata WHERE percent_mitochondrial < 0.2"
)

# 3. Download the full experiment when you need gene expression data
exp = vcpi.load_experiment("your-job-id")
seq  = exp["data"]       # gene expression matrix
meta = exp["metadata"]   # sample metadata
chem = exp["chemistry"]  # compound chemistry
```

---

## Core Concepts

### Two ways to work with data

| Approach | Function | When to use |
|---|---|---|
| **Query** | `query()` | Filter and explore metadata and chemistry — no download, no parquet |
| **Download** | `load_experiment()` | When you need gene expression data for analysis |

`query()` runs SQL against two in-memory tables — `metadata` and `chemistry` — which are fetched as lightweight API calls. The parquet file is never touched. Use it to find samples of interest, filter by compound properties, and explore experimental design before committing to a full download.

`load_experiment()` downloads the full gene expression matrix along with metadata and chemistry, and returns all three as a dict of Polars DataFrames. Use it when you need expression values.

---

## API Reference

### `list_datasets()`

Returns a Polars DataFrame of all datasets the authenticated user can access.

```python
datasets = vcpi.list_datasets()
# shape: (N, ...) — columns include job_id, name, created_at, etc.

# Get a list of job IDs
job_ids = datasets["job_id"].to_list()
```

---

### `query(job_id=None, sql="SELECT * FROM metadata LIMIT 10")`

Run SQL against `metadata` and `chemistry`. These are loaded as in-memory tables — the parquet file is never touched. Pass `job_id` to scope to one experiment, or omit it to query across all datasets you have access to.

**Available tables:** `metadata`, `chemistry`

```python
# Browse samples — no parquet, instant
df = vcpi.query(
    job_id="tvc-pgg-001",
    sql="SELECT * FROM metadata LIMIT 10"
)

# Filter by QC thresholds
df = vcpi.query(
    job_id="tvc-pgg-001",
    sql="""
        SELECT sequenced_id, user_compound_id, percent_mitochondrial
        FROM metadata
        WHERE percent_mitochondrial < 0.2
          AND is_control = false
    """
)

# Browse compounds
df = vcpi.query(
    job_id="tvc-pgg-001",
    sql="SELECT * FROM chemistry WHERE log_p < 3 AND molecular_weight < 500"
)

# Join metadata with chemistry
df = vcpi.query(
    job_id="tvc-pgg-001",
    sql="""
        SELECT m.sequenced_id, m.user_compound_id, m.timepoint,
               m.compound_concentration, c.smiles, c.log_p, c.tpsa
        FROM   metadata m
        JOIN   chemistry c ON c.compound = m.compound
        WHERE  m.cell_line = 'THP-1'
        ORDER  BY m.compound_concentration DESC
    """
)

# Cross-experiment: which compounds were tested across multiple jobs?
df = vcpi.query(
    sql="""
        SELECT user_compound_id,
               COUNT(DISTINCT job_id) AS n_experiments,
               AVG(compound_concentration) AS mean_dose_nM
        FROM   metadata
        GROUP  BY user_compound_id
        HAVING COUNT(DISTINCT job_id) > 1
        ORDER  BY n_experiments DESC
    """
)
```

---

### `describe(job_id=None)`

Returns a dict with the schema of the `metadata` and `chemistry` tables.

```python
schemas = vcpi.describe(job_id="tvc-pgg-001")

# Column names in metadata
print(schemas["metadata"]["column_name"].to_list())

# Column names in chemistry
print(schemas["chemistry"]["column_name"].to_list())
```

---

### `load_metadata(job_id)`

Fetch the experimental metadata CSV for a job as a Polars DataFrame.

```python
meta = vcpi.load_metadata("abc-123")
print(meta)
```

---

### `load_chem(job_id)`

Fetch compound chemistry data (SMILES, purity, molecular weight, logP, TPSA) for a job.

```python
chem = vcpi.load_chem("abc-123")
print(chem)
# columns: compound, user_compound_id, smiles, purity_pct,
#          molecular_weight, log_p, tpsa
```

Returns an empty DataFrame with the correct schema if no chemistry exists for the job.

---

### `load_experiment(job_id)`

Convenience function that downloads sequencing data, metadata, and chemistry in one call. Metadata and chemistry are fetched concurrently after the main download completes.

```python
exp = vcpi.load_experiment("abc-123")

exp["data"]      # Polars DataFrame — full sequencing data
exp["metadata"]  # Polars DataFrame — experimental metadata
exp["chemistry"] # Polars DataFrame — compound chemistry
exp["job_id"]    # str — the job_id you passed in

# Access individual pieces
df   = exp["data"]
chem = exp["chemistry"]
```

Returns empty DataFrames for metadata and chemistry rather than raising if those endpoints fail.

---

## Common Patterns

### Explore before you download

```python
import vcpi

# Check what's available
datasets = vcpi.list_datasets()

# Inspect the metadata and chemistry schemas
schemas = vcpi.describe("tvc-pgg-001")
print(schemas["metadata"]["column_name"].to_list())
print(schemas["chemistry"]["column_name"].to_list())

# Browse samples instantly — no download
vcpi.query("tvc-pgg-001", "SELECT * FROM metadata LIMIT 10")

# Find samples of interest, then download the full experiment
vcpi.query("tvc-pgg-001", """
    SELECT COUNT(*) AS n_samples
    FROM metadata
    WHERE percent_mitochondrial < 0.2
""")
exp = vcpi.load_experiment("tvc-pgg-001")
```

### Working with Pandas

```python
exp = vcpi.load_experiment("tvc-pgg-001")
seq_pd   = exp["data"].to_pandas()
meta_pd  = exp["metadata"].to_pandas()
chem_pd  = exp["chemistry"].to_pandas()
```

### Export filtered metadata to CSV

```python
import polars as pl

# No download needed — query() is instant
controls = vcpi.query(
    job_id="tvc-pgg-001",
    sql="""
        SELECT sequenced_id, user_compound_id, percent_mitochondrial,
               total_sequenced_reads
        FROM   metadata
        WHERE  is_control = true
          AND  percent_mitochondrial < 0.15
    """
)
controls.write_csv("control_samples_qc.csv")
```

---

## Using from R (via reticulate)

```r
library(reticulate)

# Import the package
vcpi <- import("vcpi.data")

# Helper to convert Polars -> R data.frame
polars_to_r <- function(pdf) reticulate::py_to_r(pdf$to_pandas())

# Set your token
Sys.setenv(TVC_TOKEN = "your-token-here")

# List datasets
datasets <- polars_to_r(vcpi$list_datasets())

job_id <- datasets$job_id[[1]]

# Query metadata — instant, no download
df <- polars_to_r(vcpi$query(
  job_id = job_id,
  sql    = "SELECT * FROM metadata WHERE percent_mitochondrial < 0.2 LIMIT 100"
))

# Download the full experiment
exp  <- vcpi$load_experiment(job_id)
seq  <- polars_to_r(exp[["data"]])
meta <- polars_to_r(exp[["metadata"]])
chem <- polars_to_r(exp[["chemistry"]])
```

> **Note:** `load_experiment()` shows a live progress bar in the R console via `stderr`. `query()` shows a spinner for the same reason — reticulate buffers `stdout` until the call returns, but `stderr` is flushed in real time.

---

## Data Model

Every experiment on the VCPI platform consists of three linked tables that are always collected together:

### Sequencing (`load_experiment` → `exp["data"]`)

A **wide-format gene expression matrix**. Rows are genes, columns are samples identified by `sequenced_id`.

```
shape: (~20,000 genes × ~11,811 columns)

┌──────────┬───────────┬───────────┬─────┬──────────┐
│ gene     │ 101160268 │ 101160269 │ … │ job_id   │
│ str      │ f64       │ f64       │   │ str      │
╞══════════╪═══════════╪═══════════╪═════╪══════════╡
│ BRCA1    │ 0.0       │ 12.4      │ … │ tvc-…    │
│ TP53     │ 3.1       │ 0.0       │ … │ tvc-…    │
└──────────┴───────────┴───────────┴─────┴──────────┘
```

Each numeric column is named by its `sequenced_id` — the same ID that links to the metadata table.

### Metadata (`load_metadata`)

One row per sample. Links to sequencing columns via `sequenced_id`. Contains experimental design, compound treatment, and per-sample QC metrics.

| Column | Description |
|---|---|
| `sequenced_id` | Links to the column names in the sequencing matrix |
| `job_id` | Experiment identifier |
| `compound` | UUID linking to the chemistry table |
| `user_compound_id` | Human-readable compound name (e.g. `"Bortezomib"`) |
| `compound_concentration` | Treatment dose |
| `compound_concentration_unit` | e.g. `"nM"` |
| `cell_line` | e.g. `"THP-1"` |
| `timepoint` | e.g. `"24h"` |
| `is_control` | Boolean |
| `total_sequenced_reads` | QC metric |
| `percent_mitochondrial` | QC metric — use to filter low-quality samples |
| `percent_mapped` | QC metric |
| `percent_duplicated` | QC metric |
| `ngenes3` | Number of genes with ≥ 3 counts |
| … | 26 columns total |

### Chemistry (`load_chem`)

One row per compound. Links to metadata via the `compound` UUID.

| Column | Description |
|---|---|
| `compound` | UUID — joins to `metadata.compound` |
| `user_compound_id` | Human-readable name |
| `smiles` | SMILES string |
| `molecular_weight` | g/mol |
| `log_p` | Calculated logP |
| `tpsa` | Topological polar surface area |
| `purity_pct` | Compound purity |

### How the three tables relate

```
sequencing (wide)          metadata                  chemistry
──────────────────         ──────────────────        ──────────────
gene | 101160268 | …  ←──  sequenced_id | compound ──→ compound | smiles | …
                           percent_mito | cell_line
                           job_id       | timepoint
```

The column names in the sequencing matrix are the `sequenced_id` values in metadata. Joining them requires **unpivoting** (melting) the wide matrix to long format first — see the examples below.

---

## Querying

### What `query()` does

`query()` loads `metadata` and `chemistry` as in-memory DuckDB tables and runs your SQL against them. The parquet file is never touched. This is the right tool for:

- Filtering samples by QC metrics, cell line, timepoint, or compound
- Exploring compound properties
- Cross-experiment sample discovery

For gene expression analysis, use `load_experiment()` and work locally.

### Sample and compound discovery

```python
# All THP-1 samples treated for 24h passing QC
df = vcpi.query(
    job_id="tvc-pgg-001",
    sql="""
        SELECT sequenced_id, user_compound_id, compound_concentration,
               percent_mitochondrial, total_sequenced_reads
        FROM   metadata
        WHERE  cell_line = 'THP-1'
          AND  timepoint = '24h'
          AND  percent_mitochondrial < 0.2
    """
)

# Compounds with drug-like properties (Lipinski)
df = vcpi.query(
    job_id="tvc-pgg-001",
    sql="""
        SELECT user_compound_id, molecular_weight, log_p, tpsa
        FROM   chemistry
        WHERE  log_p BETWEEN 0 AND 5
          AND  molecular_weight < 500
          AND  tpsa < 140
    """
)

# Metadata + chemistry joined
df = vcpi.query(
    job_id="tvc-pgg-001",
    sql="""
        SELECT m.sequenced_id, m.user_compound_id,
               m.compound_concentration, m.timepoint,
               c.smiles, c.log_p
        FROM   metadata m
        JOIN   chemistry c ON c.compound = m.compound
        WHERE  m.is_control = false
        ORDER  BY m.compound_concentration DESC
    """
)

# Cross-experiment: compounds tested in more than one experiment
df = vcpi.query(
    sql="""
        SELECT user_compound_id,
               COUNT(DISTINCT job_id)          AS n_experiments,
               COUNT(*)                         AS n_samples,
               AVG(compound_concentration)      AS mean_dose_nM
        FROM   metadata
        GROUP  BY user_compound_id
        HAVING COUNT(DISTINCT job_id) > 1
        ORDER  BY n_experiments DESC
    """
)
```

### Gene expression analysis (local, after download)

Gene expression requires downloading the full experiment first. The wide matrix (genes × samples) is then filtered and unpivoted locally in Polars:

```python
import vcpi
import polars as pl

exp  = vcpi.load_experiment("tvc-pgg-001")
seq  = exp["data"]       # wide: genes × samples
meta = exp["metadata"]   # one row per sample
chem = exp["chemistry"]  # one row per compound

# 1. Filter samples by QC and experimental design
good_samples = meta.filter(
    (pl.col("percent_mitochondrial") < 0.2) &
    (pl.col("cell_line") == "THP-1") &
    (pl.col("is_control") == False)
)

# 2. Select matching columns from the expression matrix
keep_cols  = ["gene"] + good_samples["sequenced_id"].cast(pl.Utf8).to_list()
seq_filtered = seq.select([c for c in keep_cols if c in seq.columns])

# 3. Unpivot to long format
seq_long = seq_filtered.unpivot(
    index="gene",
    variable_name="sequenced_id",
    value_name="expression"
).with_columns(pl.col("sequenced_id").cast(pl.Int64))

# 4. Join with metadata and chemistry
result = (
    seq_long
    .join(good_samples, on="sequenced_id")
    .join(chem, on="compound", how="left")
)

print(result.head())
```

### Cross-experiment gene expression

```python
exp1 = vcpi.load_experiment("tvc-pgg-001")
exp2 = vcpi.load_experiment("tvc-pgg-002")

def get_gene_long(exp, gene):
    seq  = exp["data"]
    meta = exp["metadata"]
    row  = seq.filter(pl.col("gene") == gene).drop(["gene", "filename"])
    long = row.unpivot(variable_name="sequenced_id", value_name="expression")
    long = long.with_columns(pl.col("sequenced_id").cast(pl.Int64))
    return long.join(
        meta.select(["sequenced_id", "job_id", "user_compound_id",
                     "compound_concentration", "is_control"]),
        on="sequenced_id"
    )

combined = pl.concat([
    get_gene_long(exp1, "TP53"),
    get_gene_long(exp2, "TP53"),
])
```

---

## Troubleshooting

**`PermissionError: TVC_TOKEN not found`**
Your token isn't set. Run `export TVC_TOKEN="your-token"` or store it in the keyring (see Authentication above).

**`401 Unauthorized`**
Your token is set but invalid or expired. Generate a new one at [thevirtualcell.com](https://thevirtualcell.com).

**`load_experiment()` is slow**
The gene expression matrix is large (~400MB). Download time depends on your network connection. The progress bar shows live download speed — typical times are 20–60 seconds on a good connection.

**`load_experiment()` fails with a timeout**
The default stream timeout is 5 minutes. For very slow connections this may not be enough. File an issue with your `job_id` and we can investigate.

---

## Dependencies

| Package | Purpose |
|---|---|
| `polars` | DataFrame engine — all return types |
| `duckdb` | SQL query engine with S3/parquet range-request support |
| `httpx` | HTTP client for streaming downloads |
| `keyring` | Secure token storage |
| `tqdm` | Download progress bar |

---

## License

MIT © Ginkgo Datapoints