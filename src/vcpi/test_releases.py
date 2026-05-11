"""End-to-end validation of every accessible VCPI release.

Lists every dataset the authenticated user can see, downloads each one,
verifies that the sample IDs in the metadata table exactly match the
sample columns in the wide-format parquet, and writes an :class:`AnnData`
object (``samples x genes``) to disk for each release.

Installed as the ``test-vcpi-releases`` console script.  Requires the
optional ``validate`` extras (``anndata``, ``numpy``):

.. code-block:: bash

    pip install "vcpi[validate]"
    test-vcpi-releases --output-dir ./vcpi-anndata
"""

from __future__ import annotations

import argparse
import contextlib
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from . import list_datasets, load_experiment

logger = logging.getLogger("vcpi.test_releases")


@dataclass
class ReleaseReport:
    """Per-release outcome for the final summary."""

    job_id: str
    job_name: str | None = None
    n_genes: int | None = None
    n_samples_metadata: int | None = None
    n_samples_parquet: int | None = None
    n_samples_intersection: int | None = None
    h5ad_path: Path | None = None
    issues: list[str] = field(default_factory=list)
    fatal: str | None = None

    @property
    def ok(self) -> bool:
        return self.fatal is None and not self.issues


def _import_anndata():
    """Import optional deps lazily with a friendly error message."""
    try:
        import anndata as ad
        import numpy as np
    except ImportError as exc:
        raise SystemExit(
            "test-vcpi-releases requires the 'validate' extras. Install with:\n"
            '    pip install "vcpi[validate]"\n'
            f"(missing: {exc.name})"
        ) from exc

    # AnnData >= 0.11 ships with a setting that gates writing of pandas
    # nullable string arrays. We rely on those (see _build_anndata) to
    # round-trip free-form metadata columns that contain missing values.
    with contextlib.suppress(AttributeError):
        ad.settings.allow_write_nullable_strings = True

    return ad, np


def _identify_gene_column(data: pl.DataFrame) -> str:
    """Pick the gene-ID column out of a wide-format expression matrix.

    The parquet has one string column (gene IDs) and N numeric columns
    (one per sample, named by ``sequenced_id``).
    """
    string_cols = [c for c in data.columns if data.schema[c] == pl.Utf8]
    if len(string_cols) == 1:
        return string_cols[0]
    # Fall back to the first non-numeric column.
    for c in data.columns:
        if not data.schema[c].is_numeric():
            return c
    raise ValueError(f"Could not identify gene-ID column in parquet with columns: {data.columns[:5]}...")


def _build_anndata(
    data: pl.DataFrame,
    metadata: pl.DataFrame,
    gene_col: str,
    sample_cols: list[str],
):
    """Construct an AnnData with samples as obs and genes as var.

    Only samples present in BOTH the metadata and the parquet are kept.
    Returns ``(adata, n_samples_kept)``.
    """
    ad, np = _import_anndata()

    gene_ids = [str(g) for g in data[gene_col].to_list()]
    counts_wide = data.select(sample_cols).to_numpy()
    # Wide parquet is genes x samples; AnnData wants samples x genes.
    X = np.asarray(counts_wide).T

    import pandas as pd

    sample_index = pd.Index([str(s) for s in sample_cols], name="sequenced_id")

    obs_pd = (
        metadata.with_columns(pl.col("sequenced_id").cast(pl.Utf8))
        .filter(pl.col("sequenced_id").is_in(sample_cols))
        .unique(subset=["sequenced_id"], keep="first")
        .to_pandas()
        .set_index("sequenced_id")
    )
    obs_pd.index = obs_pd.index.astype(str)
    obs_pd = obs_pd.reindex(sample_index)

    # Coerce object-dtype columns to pandas nullable string dtype so that
    # h5py's vlen-string writer doesn't choke on mixed None/str values.
    for col in obs_pd.columns:
        if obs_pd[col].dtype == object:
            obs_pd[col] = obs_pd[col].astype("string")

    var_pd = pd.DataFrame(index=pd.Index(gene_ids, name="gene_id"))

    return ad.AnnData(X=X, obs=obs_pd, var=var_pd), len(sample_cols)


def _validate_release(
    job_id: str,
    job_name: str | None,
    output_dir: Path,
    save_h5ad: bool,
) -> ReleaseReport:
    report = ReleaseReport(job_id=job_id, job_name=job_name)

    try:
        exp = load_experiment(job_id)
    except Exception as exc:
        report.fatal = f"load_experiment failed: {exc!r}"
        return report

    data: pl.DataFrame = exp["data"]
    metadata: pl.DataFrame = exp["metadata"]

    if data.is_empty():
        report.fatal = "parquet returned 0 rows"
        return report
    if metadata.is_empty():
        report.fatal = "metadata returned 0 rows"
        return report
    if "sequenced_id" not in metadata.columns:
        report.fatal = "metadata missing 'sequenced_id' column"
        return report

    try:
        gene_col = _identify_gene_column(data)
    except ValueError as exc:
        report.fatal = str(exc)
        return report

    parquet_samples = [c for c in data.columns if c != gene_col]
    meta_samples = metadata["sequenced_id"].cast(pl.Utf8).drop_nulls().to_list()

    parquet_set = set(parquet_samples)
    meta_set = set(meta_samples)

    only_in_parquet = sorted(parquet_set - meta_set)
    only_in_meta = sorted(meta_set - parquet_set)
    intersection = sorted(parquet_set & meta_set)

    report.n_genes = data.height
    report.n_samples_parquet = len(parquet_samples)
    report.n_samples_metadata = len(meta_samples)
    report.n_samples_intersection = len(intersection)

    if len(parquet_samples) != len(parquet_set):
        report.issues.append(
            f"parquet has duplicate sample columns ({len(parquet_samples) - len(parquet_set)} duplicates)"
        )
    if len(meta_samples) != len(meta_set):
        report.issues.append(
            f"metadata has duplicate sequenced_id rows ({len(meta_samples) - len(meta_set)} duplicates)"
        )
    if only_in_parquet:
        preview = ", ".join(only_in_parquet[:5])
        more = "" if len(only_in_parquet) <= 5 else f", ... (+{len(only_in_parquet) - 5} more)"
        report.issues.append(f"{len(only_in_parquet)} sample(s) in parquet but missing from metadata: {preview}{more}")
    if only_in_meta:
        preview = ", ".join(only_in_meta[:5])
        more = "" if len(only_in_meta) <= 5 else f", ... (+{len(only_in_meta) - 5} more)"
        report.issues.append(f"{len(only_in_meta)} sample(s) in metadata but missing from parquet: {preview}{more}")

    if not intersection:
        report.fatal = "no overlapping samples between metadata and parquet"
        return report

    try:
        adata, _ = _build_anndata(data, metadata, gene_col, intersection)
    except Exception as exc:
        report.fatal = f"AnnData construction failed: {exc!r}"
        return report

    if save_h5ad:
        output_dir.mkdir(parents=True, exist_ok=True)
        out_path = output_dir / f"{job_id}.h5ad"
        try:
            adata.write_h5ad(out_path)
            report.h5ad_path = out_path
        except Exception as exc:
            report.issues.append(f"write_h5ad failed: {exc!r}")

    return report


def _print_summary(reports: list[ReleaseReport]) -> int:
    """Print a human-readable summary. Returns the exit code."""
    print()
    print("=" * 78)
    print("VCPI release validation summary")
    print("=" * 78)

    n_ok = sum(1 for r in reports if r.ok)
    n_warn = sum(1 for r in reports if r.fatal is None and r.issues)
    n_fail = sum(1 for r in reports if r.fatal is not None)

    for r in reports:
        if r.fatal:
            status = "FAIL"
        elif r.issues:
            status = "WARN"
        else:
            status = "OK  "

        name = f" ({r.job_name})" if r.job_name and r.job_name != r.job_id else ""
        shape = ""
        if r.n_genes is not None and r.n_samples_intersection is not None:
            shape = (
                f"  genes={r.n_genes:>6}  "
                f"samples_meta={r.n_samples_metadata}/parquet={r.n_samples_parquet}"
                f"/shared={r.n_samples_intersection}"
            )
        print(f"  [{status}] {r.job_id}{name}{shape}")

        if r.fatal:
            print(f"          fatal: {r.fatal}")
        for issue in r.issues:
            print(f"          - {issue}")
        if r.h5ad_path is not None:
            print(f"          wrote {r.h5ad_path}")

    print("-" * 78)
    print(f"  {n_ok} OK   {n_warn} WARN   {n_fail} FAIL   ({len(reports)} releases)")
    print("=" * 78)

    if n_fail:
        return 2
    if n_warn:
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="test-vcpi-releases",
        description=(
            "Download every accessible VCPI release, verify that the metadata "
            "and parquet sample IDs agree, and write one AnnData (.h5ad) per "
            "release."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.cwd() / "vcpi-anndata",
        help="Directory to write .h5ad files into (default: ./vcpi-anndata).",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="Validate only; do not write .h5ad files to disk.",
    )
    parser.add_argument(
        "--job",
        action="append",
        dest="jobs",
        metavar="JOB",
        help=("Limit to a specific release (job ID or job name). May be repeated. Default: all accessible releases."),
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable INFO-level logging from the vcpi client.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Surface missing optional deps before doing any network work.
    _import_anndata()

    try:
        datasets = list_datasets()
    except Exception as exc:
        print(f"Failed to list datasets: {exc!r}", file=sys.stderr)
        return 2

    if datasets.is_empty():
        print("No datasets accessible with the current TVC_TOKEN.", file=sys.stderr)
        return 2

    rows = datasets.to_dicts()
    if args.jobs:
        wanted = set(args.jobs)
        rows = [r for r in rows if r.get("job_id") in wanted or r.get("job_name") in wanted]
        missing = wanted - {r.get("job_id") for r in rows} - {r.get("job_name") for r in rows}
        if missing:
            print(
                f"Unknown release(s): {', '.join(sorted(missing))}",
                file=sys.stderr,
            )
            return 2

    print(f"Validating {len(rows)} release(s)...")
    if not args.no_write:
        print(f"Output directory: {args.output_dir}")

    reports: list[ReleaseReport] = []
    for i, row in enumerate(rows, start=1):
        job_id = row["job_id"]
        job_name = row.get("job_name")
        label = f"{job_id}" + (f" ({job_name})" if job_name else "")
        print(f"\n[{i}/{len(rows)}] {label}")

        report = _validate_release(
            job_id=job_id,
            job_name=job_name,
            output_dir=args.output_dir,
            save_h5ad=not args.no_write,
        )
        reports.append(report)

    return _print_summary(reports)


if __name__ == "__main__":
    raise SystemExit(main())
