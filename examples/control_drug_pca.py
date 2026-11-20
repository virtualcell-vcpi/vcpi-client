"""Control-drug PCA QC for VCPI Drug-seq experiments.

This script creates one PCA plot per control compound, combining one or more
VCPI experiments while avoiding duplicate sample IDs. It is intended for local
EDA on downloaded metadata CSVs and count parquet files.

Example:
    python examples/control_drug_pca.py \
        --experiment tvc-bhr-009:/path/metadata-tvc-bhr-009.csv:/path/vcpi_tvc-bhr-009_counts.parquet \
        --experiment tvc-kdl-010:/path/metadata-tvc-kdl-010.csv:/path/vcpi_tvc-kdl-010_counts.parquet \
        --experiment tvc-qnu-012:/path/metadata-tvc-qnu-012.csv:/path/vcpi_tvc-qnu-012_counts.parquet \
        --out-dir eda_outputs/full_control_drug_pcas
"""

from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from pathlib import Path

os.environ.setdefault("XDG_CACHE_HOME", str(Path("/tmp") / "vcpi-cache"))
os.environ.setdefault("MPLCONFIGDIR", str(Path(os.environ["XDG_CACHE_HOME"]) / "matplotlib"))
Path(os.environ["XDG_CACHE_HOME"]).mkdir(parents=True, exist_ok=True)
Path(os.environ["MPLCONFIGDIR"]).mkdir(parents=True, exist_ok=True)

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


CONTROL_COMPOUNDS = [
    "DMSO",
    "Staurosporin",
    "Brefeldin-A",
    "Trichostatin-A",
    "Rigosertib",
]

QC_METRICS = [
    "total_sequenced_reads",
    "total_umi_count",
    "ngenes3",
    "n_mapped",
    "percent_mapped",
    "percent_rrna_removed",
    "percent_mitochondrial",
    "unassigned_multimapping",
    "unassigned_nofeatures",
    "percent_duplicated",
]


@dataclass(frozen=True)
class Experiment:
    job_id: str
    metadata_csv: Path
    counts_parquet: Path


def parse_experiment(value: str) -> Experiment:
    parts = value.split(":", 2)
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(
            "--experiment must have the form job_id:/path/metadata.csv:/path/counts.parquet"
        )
    job_id, metadata_csv, counts_parquet = parts
    return Experiment(job_id=job_id, metadata_csv=Path(metadata_csv), counts_parquet=Path(counts_parquet))


def slug(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_").lower()


def eta_squared(y: pd.Series | np.ndarray, group: pd.Series | np.ndarray) -> float:
    """Return one-way ANOVA R2: fraction of a PC explained by group labels."""
    values = np.asarray(y, dtype=float)
    groups = pd.Series(group).astype(str).reset_index(drop=True)
    grand = np.nanmean(values)
    ss_total = np.nansum((values - grand) ** 2)
    if ss_total <= 0:
        return float("nan")

    ss_between = 0.0
    for label in groups.unique():
        mask = (groups == label).to_numpy()
        group_values = values[mask]
        ss_between += len(group_values) * (np.nanmean(group_values) - grand) ** 2
    return float(ss_between / ss_total)


def read_counts_for_samples(counts_parquet: Path, sample_ids: list[str]) -> tuple[np.ndarray, np.ndarray]:
    """Read genes plus requested sample columns from a wide count parquet."""
    frame = pd.read_parquet(counts_parquet, columns=["gene_id", *sample_ids])
    genes = frame["gene_id"].astype(str).to_numpy()
    counts = frame[sample_ids].to_numpy(dtype=np.float64).T
    return genes, counts


def pca_from_counts(counts: np.ndarray, n_variable_genes: int) -> tuple[np.ndarray, np.ndarray, int]:
    """Library-size normalize counts, log-transform, select variable genes, run PCA."""
    library_size = counts.sum(axis=1)
    if np.any(library_size <= 0):
        raise ValueError("At least one selected sample has non-positive total count.")

    log_cpm = np.log1p(counts / library_size[:, None] * 1_000_000)
    mean_expression = log_cpm.mean(axis=0)
    variance = log_cpm.var(axis=0)
    expressed = np.where(mean_expression > 0.01)[0]

    if len(expressed) > n_variable_genes:
        selected = expressed[np.argsort(variance[expressed])[-n_variable_genes:]]
    else:
        selected = expressed

    matrix = log_cpm[:, selected]
    matrix = (matrix - matrix.mean(axis=0)) / (matrix.std(axis=0) + 1e-8)
    u, singular_values, _ = np.linalg.svd(matrix, full_matrices=False)
    scores = u[:, :5] * singular_values[:5]
    explained = (singular_values**2) / np.sum(singular_values**2)
    return scores, explained, len(selected)


def metric_associations(pca: pd.DataFrame, explained: np.ndarray) -> pd.DataFrame:
    rows = []
    for pc in ["PC1", "PC2", "PC3"]:
        pc_index = int(pc[-1]) - 1
        for metric in [*QC_METRICS, "row_id", "column_id"]:
            r = pca[pc].corr(pca[metric], method="pearson")
            rows.append(
                {
                    "pc": pc,
                    "metric": metric,
                    "association": "pearson_r",
                    "value": r,
                    "abs_value": abs(r),
                    "variance_explained": explained[pc_index],
                }
            )
        for metric in ["source_run", "job_plate", "container_id", "is_edge"]:
            r2 = eta_squared(pca[pc], pca[metric])
            rows.append(
                {
                    "pc": pc,
                    "metric": metric,
                    "association": "eta_squared_group_r2",
                    "value": r2,
                    "abs_value": r2,
                    "variance_explained": explained[pc_index],
                }
            )
    return pd.DataFrame(rows)


def plot_pca(pca: pd.DataFrame, explained: np.ndarray, associations: pd.DataFrame, drug: str, out_path: Path) -> None:
    continuous = associations[associations["association"] == "pearson_r"]
    top_pc1 = continuous[continuous["pc"] == "PC1"].sort_values("abs_value", ascending=False).iloc[0]
    top_pc2 = continuous[continuous["pc"] == "PC2"].sort_values("abs_value", ascending=False).iloc[0]

    fig, axes = plt.subplots(2, 2, figsize=(13.5, 10), dpi=160)
    axes = axes.ravel()
    fig.suptitle(f"{drug} RNA expression PCA, combined runs", fontsize=15)

    run_codes = pd.Categorical(pca["source_run"])
    axes[0].scatter(pca["PC1"], pca["PC2"], c=run_codes.codes, s=22, cmap="Set2", alpha=0.85, linewidth=0)
    axes[0].set_title("colored by run")
    for code, label in enumerate(run_codes.categories):
        denom = max(1, len(run_codes.categories) - 1)
        axes[0].scatter([], [], c=[plt.cm.Set2(code / denom)], label=label, s=35)
    axes[0].legend(frameon=False, fontsize=8)

    plate = axes[1].scatter(
        pca["PC1"], pca["PC2"], c=pca["container_id"], s=22, cmap="viridis", alpha=0.85, linewidth=0
    )
    axes[1].set_title("colored by plate/container_id")
    fig.colorbar(plate, ax=axes[1], fraction=0.046, pad=0.04).set_label("container_id")

    for axis, row in [(axes[2], top_pc1), (axes[3], top_pc2)]:
        metric = row["metric"]
        scatter = axis.scatter(
            pca["PC1"], pca["PC2"], c=pca[metric], s=22, cmap="magma", alpha=0.85, linewidth=0
        )
        axis.set_title(f"top {row['pc']} metric: {metric} r={row['value']:.2f}")
        fig.colorbar(scatter, ax=axis, fraction=0.046, pad=0.04).set_label(metric)

    for axis in axes:
        axis.set_xlabel(f"PC1 ({explained[0] * 100:.1f}% variance)")
        axis.set_ylabel(f"PC2 ({explained[1] * 100:.1f}% variance)")
        axis.grid(alpha=0.2)

    fig.tight_layout(rect=[0, 0, 1, 0.97])
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)


def load_metadata(experiments: list[Experiment]) -> pd.DataFrame:
    frames = []
    for experiment in experiments:
        frame = pd.read_csv(experiment.metadata_csv)
        frame["source_run"] = experiment.job_id
        frame["source_count_file"] = experiment.counts_parquet.name
        frame["job_plate"] = frame["job_id"].astype(str) + ":" + frame["container_id"].astype(str)
        frames.append(frame)

    metadata = pd.concat(frames, ignore_index=True)
    duplicated = metadata["sequenced_id"].astype(str).duplicated()
    if duplicated.any():
        dup_ids = metadata.loc[duplicated, "sequenced_id"].astype(str).head(10).to_list()
        raise ValueError(f"Duplicate sequenced_id values found across inputs, for example: {dup_ids}")
    return metadata


def run_control_pcas(experiments: list[Experiment], out_dir: Path, n_variable_genes: int) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    metadata = load_metadata(experiments)
    all_associations = []
    sample_counts = []

    for drug in CONTROL_COMPOUNDS:
        count_parts = []
        metadata_parts = []
        reference_genes = None

        for experiment in experiments:
            subset = metadata[
                (metadata["source_run"] == experiment.job_id)
                & metadata["is_control"]
                & (metadata["user_compound_id"].astype(str) == drug)
            ].copy()
            sample_ids = subset["sequenced_id"].astype(str).to_list()
            if not sample_ids:
                continue

            genes, counts = read_counts_for_samples(experiment.counts_parquet, sample_ids)
            if reference_genes is None:
                reference_genes = genes
            elif not np.array_equal(reference_genes, genes):
                raise ValueError(f"gene_id order mismatch for {experiment.job_id}.")

            count_parts.append(counts)
            metadata_parts.append(subset)

        if not count_parts:
            continue

        counts = np.vstack(count_parts)
        control_metadata = pd.concat(metadata_parts, ignore_index=True)
        scores, explained, n_genes_used = pca_from_counts(counts, n_variable_genes=n_variable_genes)

        pca = control_metadata[
            [
                "sequenced_id",
                "job_id",
                "source_run",
                "job_plate",
                "compound",
                "user_compound_id",
                "container_id",
                "row_id",
                "column_id",
                "is_edge",
                *QC_METRICS,
            ]
        ].copy()
        for i in range(5):
            pca[f"PC{i + 1}"] = scores[:, i]

        drug_slug = slug(drug)
        pca.to_csv(out_dir / f"{drug_slug}_full_pca_scores.csv", index=False)

        associations = metric_associations(pca, explained)
        associations.insert(0, "drug", drug)
        associations.to_csv(out_dir / f"{drug_slug}_full_pc_metric_associations.csv", index=False)
        all_associations.append(associations)

        plot_pca(pca, explained, associations, drug, out_dir / f"{drug_slug}_full_pca.png")
        sample_counts.append(
            {
                "drug": drug,
                "wells": len(pca),
                "genes_total": counts.shape[1],
                "genes_used": n_genes_used,
                "runs": pca["source_run"].nunique(),
                "plates": pca["job_plate"].nunique(),
                "pc1_variance": explained[0],
                "pc2_variance": explained[1],
                "pc3_variance": explained[2],
            }
        )

    if all_associations:
        combined = pd.concat(all_associations, ignore_index=True)
        combined.to_csv(out_dir / "all_full_control_pc_metric_associations.csv", index=False)
        top = combined.sort_values(["drug", "pc", "abs_value"], ascending=[True, True, False])
        top.groupby(["drug", "pc"]).head(8).to_csv(out_dir / "top_full_pc_metric_associations.csv", index=False)

    pd.DataFrame(sample_counts).to_csv(out_dir / "full_pca_sample_counts.csv", index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment", action="append", type=parse_experiment, required=True)
    parser.add_argument("--out-dir", type=Path, default=Path("eda_outputs/full_control_drug_pcas"))
    parser.add_argument("--n-variable-genes", type=int, default=3000)
    args = parser.parse_args()

    run_control_pcas(args.experiment, args.out_dir, args.n_variable_genes)


if __name__ == "__main__":
    main()
