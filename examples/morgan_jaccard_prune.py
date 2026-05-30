"""Prune redundant Morgan fingerprint bits by feature-column Jaccard distance.

This script starts from 2048-bit Morgan fingerprints, not the 512-bit feature
setup used in the ridge sweep. It combines training chemistry files and a test
compound CSV, then keeps one representative feature for groups of Morgan bits
whose occurrence patterns across train+test molecules have Jaccard distance
less than or equal to a threshold.

The grouping is greedy and representative-based: bits are considered in order
of decreasing prevalence, and each unassigned bit becomes a kept representative.
Unassigned bits within the Jaccard threshold of that representative are removed.
This avoids the transitive-chain problem where connected components can collapse
many features even when not every member is close to the kept bit.
"""

from __future__ import annotations

import argparse
import subprocess
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
from rdkit import Chem, DataStructs
from rdkit.Chem import rdFingerprintGenerator


CONTROL_IDS = {"DMSO", "Staurosporin", "Brefeldin-A", "Trichostatin-A", "Rigosertib"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compound-csv", action="append", required=True, help="Training chemistry CSV.")
    parser.add_argument("--test-csv", type=Path, default=None, help="Optional local test_compounds.csv.")
    parser.add_argument(
        "--test-from-git",
        default="origin/codex/qnu-rdkit-ridge-sweep:src/vcpi_prediction_contest/data_files/test_compounds.csv",
        help="git object path for test compounds if --test-csv is not supplied.",
    )
    parser.add_argument("--out-dir", type=Path, default=Path("eda_outputs/morgan_jaccard_pruning"))
    parser.add_argument("--radius", type=int, default=2)
    parser.add_argument("--fp-size", type=int, default=2048)
    parser.add_argument("--jaccard-threshold", type=float, default=0.2)
    return parser.parse_args()


def load_train(compound_csvs: list[str]) -> pd.DataFrame:
    parts = []
    for path in compound_csvs:
        job = Path(path).name.split("-2026")[0].replace("compounds-", "")
        frame = pd.read_csv(path)
        frame["source"] = "train"
        frame["job_id"] = job
        frame["compound_key"] = frame["user_compound_id"].astype(str)
        parts.append(
            frame[["source", "job_id", "compound", "compound_key", "user_compound_id", "smiles", "inchi_key"]]
        )
    train = pd.concat(parts, ignore_index=True)
    train = train[~train["compound_key"].isin(CONTROL_IDS)].copy()
    return train.drop_duplicates("compound_key", keep="first").reset_index(drop=True)


def load_test(args: argparse.Namespace) -> pd.DataFrame:
    if args.test_csv is not None:
        test = pd.read_csv(args.test_csv)
    else:
        csv_bytes = subprocess.check_output(["git", "show", args.test_from_git])
        test = pd.read_csv(BytesIO(csv_bytes))
    test["source"] = "test"
    test["job_id"] = "test_compounds"
    test["compound_key"] = test["compound"].astype(str)
    test["user_compound_id"] = test["compound"].astype(str)
    return test[["source", "job_id", "compound", "compound_key", "user_compound_id", "smiles", "inchi_key"]]


def fingerprints(chem: pd.DataFrame, *, radius: int, fp_size: int) -> tuple[pd.DataFrame, np.ndarray]:
    fp_gen = rdFingerprintGenerator.GetMorganGenerator(radius=radius, fpSize=fp_size)
    rows = []
    fps = []
    for _, row in chem.iterrows():
        mol = Chem.MolFromSmiles(str(row["smiles"])) if pd.notna(row["smiles"]) else None
        if mol is None:
            continue
        arr = np.zeros(fp_size, dtype=np.uint8)
        DataStructs.ConvertToNumpyArray(fp_gen.GetFingerprint(mol), arr)
        rows.append(row)
        fps.append(arr)
    return pd.DataFrame(rows).reset_index(drop=True), np.vstack(fps).astype(np.uint8)


def jaccard_prune(
    x: np.ndarray,
    *,
    threshold: float,
) -> tuple[np.ndarray, pd.DataFrame, pd.DataFrame]:
    bit_counts = x.sum(axis=0)
    nonzero = np.where(bit_counts > 0)[0]
    binary = x[:, nonzero].astype(np.uint16)

    intersection = (binary.T @ binary).astype(np.float32)
    counts = intersection.diagonal().copy()
    union = counts[:, None] + counts[None, :] - intersection
    similarity = np.where(union > 0, intersection / union, 0.0)
    distance = 1.0 - similarity
    np.fill_diagonal(distance, 0.0)

    prevalence = bit_counts[nonzero]
    order = np.lexsort((nonzero, -prevalence))
    assigned = np.zeros(len(nonzero), dtype=bool)
    keep_local = []
    group_rows = []

    for representative in order:
        if assigned[representative]:
            continue
        candidates = np.flatnonzero((~assigned) & (distance[representative] <= threshold))
        assigned[candidates] = True
        keep_local.append(representative)
        member_bits = nonzero[candidates]
        distances = distance[representative, candidates]
        group_rows.append(
            {
                "group_id": len(group_rows),
                "kept_bit": int(nonzero[representative]),
                "kept_prevalence": int(bit_counts[nonzero[representative]]),
                "n_features": int(len(candidates)),
                "max_distance_to_kept": float(distances.max() if len(distances) else 0.0),
                "mean_distance_to_kept": float(distances.mean() if len(distances) else 0.0),
                "member_bits": ";".join(map(str, sorted(map(int, member_bits)))),
            }
        )

    keep_local = np.array(sorted(keep_local), dtype=int)
    kept_bits = nonzero[keep_local]
    groups = pd.DataFrame(group_rows).sort_values(["n_features", "kept_bit"], ascending=[False, True])
    summary = pd.DataFrame(
        [
            {"metric": "nonzero_bits", "value": len(nonzero)},
            {"metric": "kept_bits", "value": len(kept_bits)},
            {"metric": "redundant_nonzero_bits_removed", "value": len(nonzero) - len(kept_bits)},
            {"metric": "largest_group_size", "value": int(groups["n_features"].max())},
            {"metric": "groups_with_more_than_one_feature", "value": int((groups["n_features"] > 1).sum())},
        ]
    )
    return kept_bits, groups, summary


def main() -> None:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    chem = pd.concat([load_train(args.compound_csv), load_test(args)], ignore_index=True)
    chem = chem.drop_duplicates(["source", "compound_key"], keep="first").reset_index(drop=True)
    metadata, x = fingerprints(chem, radius=args.radius, fp_size=args.fp_size)
    kept_bits, groups, pruning_summary = jaccard_prune(x, threshold=args.jaccard_threshold)

    reduced = pd.DataFrame(x[:, kept_bits], columns=[f"morgan_{bit}" for bit in kept_bits])
    reduced.insert(0, "source", metadata["source"].to_numpy())
    reduced.insert(1, "compound_key", metadata["compound_key"].astype(str).to_numpy())
    reduced.insert(2, "smiles", metadata["smiles"].astype(str).to_numpy())

    metadata.to_csv(args.out_dir / "morgan_train_test_compound_metadata.csv", index=False)
    pd.DataFrame({"kept_bit": kept_bits.astype(int)}).to_csv(
        args.out_dir / "morgan2048_jaccard_leq0p2_kept_bits.csv", index=False
    )
    groups.to_csv(args.out_dir / "morgan2048_jaccard_leq0p2_feature_groups.csv", index=False)
    reduced.to_parquet(args.out_dir / "morgan2048_jaccard_leq0p2_reduced_features.parquet", index=False)

    run_summary = pd.DataFrame(
        [
            {"metric": "train_compounds_valid", "value": int((metadata["source"] == "train").sum())},
            {"metric": "test_compounds_valid", "value": int((metadata["source"] == "test").sum())},
            {"metric": "morgan_radius", "value": args.radius},
            {"metric": "morgan_fp_size_started", "value": args.fp_size},
            {"metric": "jaccard_distance_threshold", "value": args.jaccard_threshold},
            {
                "metric": "grouping_method",
                "value": "greedy representative; removed bits must be within threshold of kept bit",
            },
        ]
    )
    pd.concat([run_summary, pruning_summary], ignore_index=True).to_csv(
        args.out_dir / "morgan2048_jaccard_leq0p2_summary.csv", index=False
    )


if __name__ == "__main__":
    main()
