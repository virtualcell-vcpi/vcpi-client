"""
vcpi_client.py
--------------
A Python client for the TVC sequencing data platform.

Authentication:
    Set TVC_TOKEN via environment variable or store it with vcpi.login().

Example:
    import vcpi_client as vcpi
    datasets = vcpi.list_datasets()

    # Explore without downloading — fast, uses DuckDB range requests against S3
    df = vcpi.query(job="tvc-bhr-009", sql="SELECT * FROM dataset LIMIT 5")
    df = vcpi.query(job="vcpi-0001", sql="SELECT * FROM dataset LIMIT 5")  # by name

    # Download the full dataset when you need everything locally
    df = vcpi.load_dataset("tvc-bhr-009")
"""

from __future__ import annotations

import concurrent.futures
import io
import logging
import os
import sys
import tempfile

import duckdb
import httpx
import keyring
import polars as pl
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SUPABASE_FUNCTIONS_URL = "https://pdexvrcgdabfgnkpgqpu.supabase.co/functions/v1"

# This is a CLIENT-SIDE PUBLISHABLE KEY — safe to ship in package source,
# the same way Stripe's pk_live_... or a Firebase apiKey is public.
# It identifies the application to Supabase but grants no privileges on its own.
# The user's TVC_TOKEN (below) is the actual secret that controls data access.
SUPABASE_KEY: str = "sb_publishable_Q6Mr49QEXcc4cu64ebdArg_26DRBnuj"

# Memory thresholds
_WARN_BYTES = 2 * 1024 ** 3   # 2 GB — warn but proceed
_ABORT_BYTES = 8 * 1024 ** 3  # 8 GB — refuse to join, return list fallback

# ---------------------------------------------------------------------------
# Timeouts (seconds)
# ---------------------------------------------------------------------------
TIMEOUT_METADATA = 30.0  # lightweight metadata / chemistry calls
TIMEOUT_DATASET = 60.0  # single-dataset URL resolution
TIMEOUT_STREAM = 300.0  # full parquet download

# ---------------------------------------------------------------------------
# Shared empty chemistry schema — defined once, reused everywhere
# All molecular properties computed via RDKit
# ---------------------------------------------------------------------------
EMPTY_CHEM_DF = pl.DataFrame(
    {
        "compound": pl.Series([], dtype=pl.Utf8),
        "user_compound_id": pl.Series([], dtype=pl.Utf8),
        "smiles": pl.Series([], dtype=pl.Utf8),
        "purity_pct": pl.Series([], dtype=pl.Float64),
        "molecular_weight": pl.Series([], dtype=pl.Float64),
        "log_p": pl.Series([], dtype=pl.Float64),
        "tpsa": pl.Series([], dtype=pl.Float64),
        "inchi_key": pl.Series([], dtype=pl.Utf8),
        "num_rotatable_bonds": pl.Series([], dtype=pl.Int64),
        "num_h_acceptors": pl.Series([], dtype=pl.Int64),
        "num_h_donors": pl.Series([], dtype=pl.Int64),
        "num_atoms": pl.Series([], dtype=pl.Int64),
        "num_bonds": pl.Series([], dtype=pl.Int64),
    }
)

# ---------------------------------------------------------------------------
# Session-level token cache — avoids hitting keyring on every request
# ---------------------------------------------------------------------------
_cached_token: str | None = None


def _clear_token_cache() -> None:
    global _cached_token
    _cached_token = None


# ---------------------------------------------------------------------------
# Session-level datasets cache — populated once per process by _resolve_job()
# ---------------------------------------------------------------------------
_datasets_cache: list[dict] | None = None


def _clear_datasets_cache() -> None:
    global _datasets_cache
    _datasets_cache = None


def _resolve_job(job: str) -> str:
    """Resolve a job name or job ID to a canonical job_id.

    Accepts either the opaque identifier (e.g. ``"tvc-bhr-009"``) or the
    human-readable name (e.g. ``"vcpi-0001"``).  The datasets list is fetched
    once and cached for the lifetime of the process.
    """
    global _datasets_cache
    if _datasets_cache is None:
        _datasets_cache = list_datasets().to_dicts()

    # Exact match on job_id first (most common path — users copy IDs)
    for d in _datasets_cache:
        if d["job_id"] == job:
            return job

    # Exact match on job_name
    matches = [d for d in _datasets_cache if d.get("job_name") == job]
    if len(matches) == 1:
        return matches[0]["job_id"]
    if len(matches) > 1:
        ids = [m["job_id"] for m in matches]
        raise ValueError(f"Ambiguous job name {job!r} matches multiple datasets: {ids}")

    raise ValueError(
        f"No dataset found matching {job!r}. "
        "Run vcpi.list_datasets() to see available job IDs and names."
    )


def _get_token() -> str:
    """Retrieve the bearer token, caching it for the lifetime of the process."""
    global _cached_token
    if not _cached_token:
        _cached_token = os.environ.get("TVC_TOKEN") or keyring.get_password("vcpi-client", "TVC_TOKEN")
    if not _cached_token:
        raise PermissionError("TVC_TOKEN not found. Please run vcpi.login() or set the TVC_TOKEN environment variable.")
    return _cached_token


def _headers() -> dict[str, str]:
    """Build authentication headers for every Edge Function request."""
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {_get_token()}",
        "Content-Type": "application/json",
    }


def _make_duckdb_con() -> duckdb.DuckDBPyConnection:
    """
    Return an in-memory DuckDB connection with httpfs loaded.
    Attempts IF NOT EXISTS first (DuckDB >= 0.9); falls back to a bare
    INSTALL for older versions where the syntax is not supported.
    """
    con = duckdb.connect(database=":memory:")
    try:
        con.execute("INSTALL httpfs IF NOT EXISTS; LOAD httpfs;")
    except duckdb.ParserException:
        con.execute("INSTALL httpfs; LOAD httpfs;")
    return con


def _safe_load_chem(job_id: str) -> pl.DataFrame:
    """
    Fault-tolerant wrapper around load_chem().
    Returns an empty DataFrame instead of raising, so a missing
    chemistry dataset never aborts a larger query.
    """
    try:
        return load_chem(job_id)
    except Exception as exc:
        logger.warning("Could not load chemistry for %s: %s", job_id, exc)
        return EMPTY_CHEM_DF


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def list_datasets() -> pl.DataFrame:
    """
    List every dataset the authenticated user is authorised to access.

    Returns
    -------
    pl.DataFrame
        One row per dataset with id, name, and access metadata columns.
    """
    with httpx.Client(timeout=TIMEOUT_METADATA) as client:
        resp = client.get(
            f"{SUPABASE_FUNCTIONS_URL}/list-datasets",
            headers=_headers(),
        )
        resp.raise_for_status()
    return pl.DataFrame(resp.json()["datasets"])


def resolve_dataset_url(job: str) -> str:
    """
    Resolve and return the signed parquet URL for a job.
    Separated from the download so R can own the streaming + progress bar,
    passing the downloaded file path back via read_parquet_file().
    """
    job_id = _resolve_job(job)
    with httpx.Client(timeout=TIMEOUT_DATASET) as client:
        resp = client.get(
            f"{SUPABASE_FUNCTIONS_URL}/get-dataset",
            params={"job_id": job_id},
            headers=_headers(),
        )
        resp.raise_for_status()
        data_url: str | None = resp.json().get("parquet_url")

    if not data_url:
        raise ValueError(f"No parquet URL returned for job_id: {job_id!r}")

    return data_url


def load_dataset(job: str) -> pl.DataFrame:
    """
    Download the full sequencing dataset for a single experiment.

    For exploration (previewing rows, filtering, aggregating), use
    :func:`query` instead — it runs SQL directly against the remote
    parquet file via DuckDB and never downloads the full file.

    Parameters
    ----------
    job:
        The experiment identifier or human-readable name returned by
        :func:`list_datasets`.  Either the job ID (e.g. ``"tvc-bhr-009"``)
        or the job name (e.g. ``"vcpi-0001"``) is accepted.

    Returns
    -------
    pl.DataFrame
    """
    job_id = _resolve_job(job)
    data_url = resolve_dataset_url(job_id)
    logger.info("Downloading dataset for %s", job_id)

    with httpx.Client(timeout=TIMEOUT_STREAM) as client, client.stream("GET", data_url) as stream:
        total = int(stream.headers.get("Content-Length", 0))
        with (
            tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp,
            tqdm(
                total=total,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=f"Downloading {job_id[:8]}…",
                file=sys.stderr,  # stderr bypasses reticulate buffering
                dynamic_ncols=True,
                miniters=1,
                smoothing=0.1,
            ) as progress,
        ):
            tmp_path = tmp.name
            for chunk in stream.iter_bytes(chunk_size=1024 * 256):
                tmp.write(chunk)
                progress.update(len(chunk))

    try:
        return pl.read_parquet(tmp_path)
    finally:
        os.unlink(tmp_path)


def load_metadata(job: str) -> pl.DataFrame:
    """
    Fetch experimental metadata for a single job as a Polars DataFrame.

    Parameters
    ----------
    job:
        The experiment identifier or human-readable name.

    Returns
    -------
    pl.DataFrame
    """
    job_id = _resolve_job(job)
    with httpx.Client(timeout=TIMEOUT_METADATA) as client:
        resp = client.get(
            f"{SUPABASE_FUNCTIONS_URL}/download-dataset-metadata",
            params={"job_id": job_id},
            headers=_headers(),
        )
        resp.raise_for_status()
    return pl.read_csv(io.BytesIO(resp.content))


def load_chem(job: str) -> pl.DataFrame:
    """
    Fetch compound chemistry data for a single job.

    Returns an empty DataFrame with the correct schema when no chemistry
    data exists for the given job (HTTP 404).

    All molecular properties are computed via RDKit.

    Parameters
    ----------
    job:
        The experiment identifier or human-readable name.

    Returns
    -------
    pl.DataFrame
        Columns: ``compound``, ``user_compound_id``, ``smiles``,
        ``purity_pct``, ``molecular_weight``, ``log_p``, ``tpsa``,
        ``inchi_key``, ``num_rotatable_bonds``, ``num_h_acceptors``,
        ``num_h_donors``, ``num_atoms``, ``num_bonds``.
    """
    job_id = _resolve_job(job)
    with httpx.Client(timeout=TIMEOUT_METADATA) as client:
        resp = client.get(
            f"{SUPABASE_FUNCTIONS_URL}/get-dataset-compounds",
            params={"job_id": job_id},
            headers=_headers(),
        )
        if resp.status_code in (404, 500):
            # 404 = no chemistry for this job
            # 500 = server can't resolve an unknown job_id — treat as missing
            return EMPTY_CHEM_DF
        resp.raise_for_status()

    compounds = resp.json().get("compounds", [])
    return pl.DataFrame(compounds) if compounds else EMPTY_CHEM_DF


def query(
    job: str | None = None,
    sql: str = "SELECT * FROM metadata LIMIT 10",
) -> pl.DataFrame:
    """
    Execute a SQL query across two named tables: ``metadata`` and
    ``chemistry``. These are fetched as lightweight API calls and never
    touch the parquet file on S3.

    For gene expression work, use :func:`load_experiment` to download
    the full triad locally, then filter and join in Polars.

    Tables available in every query
    --------------------------------
    **metadata**
        One row per sample. Columns include ``sequenced_id``, ``job_id``,
        ``compound``, ``user_compound_id``, ``cell_line``, ``timepoint``,
        ``is_control``, ``percent_mitochondrial``, ``percent_mapped``,
        ``total_sequenced_reads``, and ~20 other QC / experimental fields.

    **chemistry**
        One row per compound. Molecular properties computed via RDKit.
        Columns: ``compound``, ``user_compound_id``, ``smiles``,
        ``purity_pct``, ``molecular_weight``, ``log_p``, ``tpsa``,
        ``inchi_key``, ``num_rotatable_bonds``, ``num_h_acceptors``,
        ``num_h_donors``, ``num_atoms``, ``num_bonds``.
        Join to metadata on ``compound``.

    Parameters
    ----------
    job:
        Restrict both tables to a single experiment.  Accepts either the
        job ID (e.g. ``"tvc-bhr-009"``) or the human-readable job name
        (e.g. ``"vcpi-0001"``).  Pass ``None`` to query across every
        dataset the authenticated user can access.
    sql:
        DuckDB-compatible SQL.  Available tables: ``metadata``,
        ``chemistry``.

        .. warning::
            ``sql`` is executed directly.  Do **not** pass untrusted input.

    Returns
    -------
    pl.DataFrame
    """
    # ---------------------------------------------------------------------------
    # 1. Resolve manifest (parquet URLs + job_ids)
    # ---------------------------------------------------------------------------
    job_id = _resolve_job(job) if job is not None else None
    with httpx.Client(timeout=TIMEOUT_DATASET) as client:
        if job_id:
            resp = client.get(
                f"{SUPABASE_FUNCTIONS_URL}/get-dataset",
                params={"job_id": job_id},
                headers=_headers(),
            )
            if resp.status_code == 404:
                raise ValueError(
                    f"Dataset not found for job_id={job_id!r}. Run vcpi.list_datasets() to see available datasets."
                )
            resp.raise_for_status()
            manifest = [resp.json()]
        else:
            resp = client.get(
                f"{SUPABASE_FUNCTIONS_URL}/list-authorized-urls",
                headers=_headers(),
            )
            resp.raise_for_status()
            manifest = resp.json().get("urls", [])

    if not manifest:
        logger.warning("query(): manifest is empty — returning empty DataFrame.")
        return pl.DataFrame()

    job_ids = [m["job_id"] for m in manifest if m.get("job_id")]

    # ---------------------------------------------------------------------------
    # 2. Fetch metadata + chemistry concurrently — these are fast API calls
    #    that never touch the parquet.
    # ---------------------------------------------------------------------------
    spinner_chars = ["|", "/", "-", "\\"]
    spinner_i = 0

    def _tick(label: str) -> None:
        nonlocal spinner_i
        sys.stderr.write(f"\r  {label}… {spinner_chars[spinner_i % len(spinner_chars)]}  ")
        sys.stderr.flush()
        spinner_i += 1

    _tick("Fetching metadata")

    def _fetch_metadata(jid: str) -> pl.DataFrame:
        try:
            return load_metadata(jid)
        except Exception as exc:
            logger.warning("Could not load metadata for %s: %s", jid, exc)
            return pl.DataFrame()

    with concurrent.futures.ThreadPoolExecutor() as pool:
        meta_frames = list(pool.map(_fetch_metadata, job_ids))
        _tick("Fetching chemistry")
        chem_frames = list(pool.map(_safe_load_chem, job_ids))

    meta_frames = [df for df in meta_frames if not df.is_empty()]
    chem_frames = [df for df in chem_frames if not df.is_empty()]

    meta_df = pl.concat(meta_frames) if meta_frames else pl.DataFrame()
    chem_df = pl.concat(chem_frames) if chem_frames else EMPTY_CHEM_DF

    # ---------------------------------------------------------------------------
    # 3. Build DuckDB session and register fast tables
    # ---------------------------------------------------------------------------
    _tick("Building tables")
    con = _make_duckdb_con()
    con.register("metadata", meta_df)
    con.register("chemistry", chem_df)

    # sequencing is intentionally NOT registered as a queryable table.
    #
    # The parquet is wide-format (genes × samples). DuckDB cannot push
    # WHERE/filter predicates through an UNPIVOT, so any SQL against a
    # sequencing view would download the entire parquet regardless of how
    # narrow the query is. There is no benefit over load_dataset().
    #
    # For gene expression work: use load_experiment() to get the full triad
    # locally, then filter/join in Polars.

    # ---------------------------------------------------------------------------
    # 4. Run query in background thread; spinner on main thread via stderr
    # ---------------------------------------------------------------------------
    result: list = []
    error: list = []

    def _run() -> None:
        try:
            result.append(con.execute(sql).pl())
        except Exception as exc:
            error.append(exc)

    thread = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = thread.submit(_run)

    while True:
        try:
            future.result(timeout=0.1)
            break
        except concurrent.futures.TimeoutError:
            _tick("Querying")

    sys.stderr.write("\r  Query complete.          \n")
    sys.stderr.flush()

    if error:
        raise error[0]
    return result[0]


def describe(job: str | None = None) -> dict[str, pl.DataFrame]:
    """
    Return the schema of the ``metadata`` and ``chemistry`` tables.

    Parameters
    ----------
    job:
        Scope to a single experiment.  Accepts either the job ID or the
        human-readable job name.  ``None`` uses the full collective.

    Returns
    -------
    dict with keys ``"metadata"`` and ``"chemistry"``,
        each containing a DataFrame of column names and types.
    """
    return {
        "metadata": query(job, "DESCRIBE metadata"),
        "chemistry": query(job, "DESCRIBE chemistry"),
    }


def load_experiment(job: str) -> dict[str, pl.DataFrame | str]:
    """
    Convenience loader: fetch sequencing data, metadata, and chemistry for
    one experiment in a single call.  Metadata and chemistry are fetched
    concurrently while the sequencing data downloads.

    For exploration without a full download, use :func:`query` instead.

    Parameters
    ----------
    job:
        The experiment identifier or human-readable name.

    Returns
    -------
    dict with keys:
        * ``"data"``      — sequencing :class:`pl.DataFrame`
        * ``"metadata"``  — metadata :class:`pl.DataFrame` (empty on failure)
        * ``"chemistry"`` — chemistry :class:`pl.DataFrame` (empty on failure)
        * ``"job_id"``    — the resolved job ID string
    """
    job_id = _resolve_job(job)
    print(f"\n--- Loading experiment: {job_id} ---")

    # Sequencing data is the heavy lift — download first
    dataset = load_dataset(job_id)

    # Metadata + chemistry are lightweight and independent — fetch together
    def _safe_load_metadata() -> pl.DataFrame:
        try:
            return load_metadata(job_id)
        except Exception as exc:
            logger.warning("Could not load metadata for %s: %s", job_id, exc)
            return pl.DataFrame()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        meta_future = pool.submit(_safe_load_metadata)
        chem_future = pool.submit(_safe_load_chem, job_id)
        metadata = meta_future.result()
        chemistry = chem_future.result()

    return {
        "data": dataset,
        "metadata": metadata,
        "chemistry": chemistry,
        "job_id": job_id,
    }

def _estimate_dataframe_bytes(df: pl.DataFrame) -> int:
    """Estimate in-memory size of a Polars DataFrame in bytes."""
    return sum(df[col].estimated_size() for col in df.columns)


def load_experiments(
    jobs: list[str],
    sql: str | None = None,
) -> dict[str, pl.DataFrame | list[str]]:
    """
    Load and merge multiple experiments, retaining only expressed genes.

    Downloads the full sequencing parquet for each experiment, optionally
    filters samples via a SQL query against metadata, then removes genes
    with zero expression across all samples in the combined set before
    joining horizontally.

    This is a convenience function for cross-experiment analysis. For the
    full unfiltered matrix of a single experiment (zeros included), use
    :func:`load_experiment`. For metadata and chemistry queries without
    downloading parquets, use :func:`query`.

    Parameters
    ----------
    jobs:
        List of experiment identifiers or human-readable names
        (e.g. ``["vcpi-0001", "vcpi-0002"]``).
    sql:
        Optional SQL WHERE clause (or full SELECT) passed to :func:`query`
        to filter samples before joining. Available tables: ``metadata``,
        ``chemistry``. Example: ``"SELECT * FROM metadata WHERE
        user_compound_id = 'DMSO'"``.

    Returns
    -------
    dict with keys:
        * ``"data"``      — horizontally joined sequencing :class:`pl.DataFrame`,
                            expressed genes only (zero-only columns removed)
        * ``"metadata"``  — concatenated metadata :class:`pl.DataFrame`
        * ``"chemistry"`` — concatenated chemistry :class:`pl.DataFrame`
        * ``"job_ids"``   — list of resolved job ID strings
        * ``"fallback"``  — ``True`` if memory limits prevented joining;
                            in that case ``"data"`` is a dict of
                            ``{job_id: pl.DataFrame}`` instead of a single frame

    Warnings
    --------
    A warning is emitted if the estimated combined size exceeds 2 GB.
    If the estimated size exceeds 8 GB the join is skipped entirely and
    individual frames are returned under the ``"data"`` key as a dict.
    """
    results = [load_experiment(j) for j in jobs]
    job_ids = [r["job_id"] for r in results]

    # ── Metadata + chemistry ────────────────────────────────────────────────
    meta_frames = [r["metadata"] for r in results if not r["metadata"].is_empty()]
    metadata = pl.concat(meta_frames) if meta_frames else pl.DataFrame()

    chem_frames = [r["chemistry"] for r in results if not r["chemistry"].is_empty()]
    chemistry = pl.concat(chem_frames) if chem_frames else EMPTY_CHEM_DF

    # ── Optional sample filter via query() ──────────────────────────────────
    sample_ids: set[str] | None = None
    if sql is not None:
        filtered_meta = query(sql=sql)
        if "sequenced_id" not in filtered_meta.columns:
            raise ValueError(
                "sql filter must return a 'sequenced_id' column. "
                "Use: SELECT * FROM metadata WHERE ..."
            )
        sample_ids = set(str(s) for s in filtered_meta["sequenced_id"].to_list())
        metadata = metadata.filter(pl.col("sequenced_id").is_in(sample_ids))

    # ── Filter data frames to requested samples ─────────────────────────────
    data_frames: dict[str, pl.DataFrame] = {}
    gene_col: str | None = None

    for r in results:
        df = r["data"]
        jid = r["job_id"]

        # Identify gene ID column — the one shared column across all releases
        if gene_col is None:
            all_cols = [set(r2["data"].columns) for r2 in results]
            shared = list(all_cols[0].intersection(*all_cols[1:]))
            if len(shared) != 1:
                raise ValueError(
                    f"Expected exactly one shared column (gene ID), found {len(shared)}: {shared}"
                )
            gene_col = shared[0]

        # Filter to requested samples if sql was provided
        if sample_ids is not None:
            keep_cols = [gene_col] + [c for c in df.columns if c in sample_ids]
            df = df.select(keep_cols)

        # Drop genes with zero expression across all samples in this frame
        sample_cols = [c for c in df.columns if c != gene_col]
        if sample_cols:
            expressed_mask = (
                df.select(sample_cols)
                .select(pl.all_horizontal(pl.all() == 0))
                .to_series()
                .not_()
            )
            df = df.filter(expressed_mask)

        data_frames[jid] = df

    # ── Memory check ────────────────────────────────────────────────────────
    estimated_bytes = sum(_estimate_dataframe_bytes(df) for df in data_frames.values())
    estimated_gb = estimated_bytes / 1024 ** 3

    if estimated_bytes > _ABORT_BYTES:
        import warnings
        warnings.warn(
            f"Combined data estimated at {estimated_gb:.1f} GB, which exceeds the "
            f"{_ABORT_BYTES // 1024**3} GB join limit. Returning individual frames "
            f"as a dict instead of a joined DataFrame. Access via result['data'][job_id].",
            ResourceWarning,
            stacklevel=2,
        )
        return {
            "data": data_frames,
            "metadata": metadata,
            "chemistry": chemistry,
            "job_ids": job_ids,
            "fallback": True,
        }

    if estimated_bytes > _WARN_BYTES:
        import warnings
        warnings.warn(
            f"Combined data estimated at {estimated_gb:.1f} GB. "
            f"This may be slow or cause memory pressure on smaller machines.",
            ResourceWarning,
            stacklevel=2,
        )

    # ── Horizontal join on gene ID column ───────────────────────────────────
    try:
        data = list(data_frames.values())[0]
        for df in list(data_frames.values())[1:]:
            data = data.join(df, on=gene_col, how="full", coalesce=True)

        return {
            "data": data,
            "metadata": metadata,
            "chemistry": chemistry,
            "job_ids": job_ids,
            "fallback": False,
        }

    except Exception as exc:
        import warnings
        warnings.warn(
            f"Join failed ({exc}). Returning individual frames as a dict instead. "
            f"Access via result['data'][job_id].",
            ResourceWarning,
            stacklevel=2,
        )
        return {
            "data": data_frames,
            "metadata": metadata,
            "chemistry": chemistry,
            "job_ids": job_ids,
            "fallback": True,
        }
