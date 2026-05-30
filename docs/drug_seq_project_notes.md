# Drug-seq Project Notes

These notes summarize the practical dataset understanding and early EDA findings
from the VCPI Drug-seq modeling discussion.

## Task Framing

The core modeling task is to predict transcriptomic response to an unseen
compound. In practical terms:

```text
compound representation -> predicted gene-expression response vector
```

The response should be modeled as a perturbation effect, such as normalized
treated expression minus DMSO/control expression, rather than raw counts.

The evaluation target is compound generalization. Validation splits should
therefore be compound-held-out, and preferably scaffold-held-out when using
chemical structures.

## Dataset Pieces

The dataset has three key table types.

### Metadata

One row per sequenced well/sample.

Important columns:

- `sequenced_id`: sample/well ID; this matches the count-matrix sample columns.
- `job_id`: experiment/run ID, such as `tvc-bhr-009`, `tvc-kdl-010`, or `tvc-qnu-012`.
- `container_id`: physical assay plate ID.
- `row_id`, `column_id`: well position on the plate.
- `is_edge`: whether a well is on the outer edge of the plate.
- `compound`: internal compound UUID.
- `user_compound_id`: human-facing compound ID or control name.
- `compound_concentration`: dose.
- `compound_concentration_unit`: dose unit, usually `nM` for drugs and `%` for DMSO.
- `cell_line`: THP-1 in these files.
- `timepoint`: 24h in these files.
- `is_control`: whether the well is a control.
- `total_umi_count`: total unique RNA molecules detected in the well.
- `ngenes3`: number of genes detected with at least 3 UMIs.
- `percent_mapped`: percent of reads that mapped successfully.
- `percent_rrna_removed`: ribosomal-RNA-related filtering/removal metric.
- `percent_mitochondrial`: mitochondrial RNA fraction; high values can indicate stressed or damaged cells.
- `percent_duplicated`: duplicate-read fraction, related to library complexity.

Definitions:

- UMI means unique molecular identifier. It is a barcode used to count original
  RNA molecules while reducing PCR duplicate inflation.
- A mapped read is a sequencing read that can be assigned to the reference
  genome or transcriptome.
- A feature usually means an annotated gene or transcript region.

### Compound/Chemistry

One row per compound.

Important columns:

- `compound`: internal compound UUID; joins to metadata.
- `user_compound_id`: external/user-facing ID; controls appear as names like DMSO.
- `smiles`: text representation of chemical structure.
- `inchi_key`: standardized chemical identifier, useful for detecting duplicate chemistry.
- `molecular_weight`: compound mass.
- `log_p`: hydrophobicity/lipophilicity measure.
- `tpsa`: topological polar surface area.
- `num_rotatable_bonds`: molecular flexibility.
- `num_h_acceptors`, `num_h_donors`: hydrogen-bonding features.
- `num_atoms`, `num_bonds`: rough size/complexity measures.

Definitions:

- SMILES is a compact string notation for molecular structure.
- InChIKey is a standardized hashed chemical identifier.
- Higher `log_p` usually means more fat-soluble; lower `log_p` means more water-soluble.
- Higher TPSA usually means more polar, which can reduce cell permeability.

### Counts Parquet

The count parquet is a wide gene-expression matrix:

```text
rows: gene_id
columns: sequenced_id sample columns
values: gene-level UMI counts
```

For `tvc-qnu-012`, the count file had:

```text
78,778 genes/features
21,888 sample columns
```

The metadata `sequenced_id` values matched the count columns exactly.

## Controls

The controls serve different purposes:

- DMSO: negative/vehicle control. Use this as the neutral baseline.
- Staurosporin: strong positive control, often used as a cell-death/stress control.
- Brefeldin-A, Trichostatin-A, Rigosertib: transcriptional positive controls.

Do not use Staurosporin or the other active controls as neutral baselines. They
are biologically active and should be used to verify assay behavior.

## PCA/QC Findings

We generated one PCA per control compound across three jobs:

- DMSO
- Staurosporin
- Brefeldin-A
- Trichostatin-A
- Rigosertib

The rendered figures are included in
[`docs/control_drug_pca_eda.md`](control_drug_pca_eda.md). The image files live
under `docs/figures/control_drug_pcas/`.

Before combining, duplicate checks found:

- No overlapping `sequenced_id` values across the three metadata files.
- Each count parquet matched its metadata sample IDs exactly.
- The expected shared compounds across chemistry files were the five controls.

Main result: the first PCs in control wells are strongly associated with
technical variation, especially plate and QC metrics.

Key drivers included:

- `container_id` / `job_plate`: physical plate identity.
- `ngenes3`: gene detection depth.
- `percent_duplicated`: library complexity.
- `percent_mapped`: mapping quality.
- `percent_rrna_removed`: rRNA-related processing.
- `percent_mitochondrial`: cell stress/sample quality.
- `unassigned_nofeatures`: mapped reads not assigned to annotated genes.

The clearest warning case was Staurosporin:

```text
PC1 variance explained: 26.52%
job_plate eta-squared: 0.985
ngenes3 correlation with PC1: 0.973
percent_rrna_removed correlation with PC1: -0.916
```

This means Staurosporin control profiles vary strongly by plate and QC metrics.

After regressing `ngenes3` out of each gene's log-normalized expression, the
Staurosporin PC1 variance dropped from 26.52% to 1.84%, and the PC1 correlation
with `ngenes3` went to approximately zero. This confirms that the original
Staurosporin PC1 was mostly a gene-detection-depth axis. Residual PCs were still
associated with other QC metrics such as `percent_duplicated`, `percent_mapped`,
and `percent_rrna_removed`.

## What `container_id` Means

`container_id` is the physical assay plate ID. Each `sequenced_id` is one well,
while `container_id` says which plate that well came from.

In these data, plate layout appears to be 384-well format:

```text
row_id: 1 to 16
column_id: 1 to 24
16 x 24 = 384 wells
```

Plate has a large PCA effect because wells on the same plate share technical
conditions: cell handling, liquid handling, incubation, evaporation, RNA capture,
library prep, and sequencing quality. If controls cluster by plate, the dataset
contains plate/batch effects that must be accounted for before modeling.

## Recommended Preprocessing

A conservative first preprocessing plan:

1. Filter or flag low-quality wells using `total_umi_count`, `ngenes3`,
   `percent_mapped`, `percent_mitochondrial`, `percent_duplicated`, and PCA
   outliers.
2. Normalize counts by library size, such as counts per million.
3. Apply `log1p` transform.
4. Use DMSO controls as the neutral baseline.
5. Correct plate effects using DMSO-anchored mean-shift correction.
6. Compute compound-level response signatures from treated replicates relative
   to same-plate or corrected DMSO baseline.
7. Train models with compound-held-out validation.

## DMSO-Anchored Mean-Shift Correction

For each plate and gene:

```text
corrected_expression =
    expression
    - plate_DMSO_mean
    + global_DMSO_mean
```

This shifts each plate so its DMSO baseline matches the global DMSO baseline.
It uses DMSO because DMSO is the neutral vehicle control. The active controls
should not be used to define the neutral baseline.

## Modeling Plan

Start with a simple baseline before adding architecture complexity:

```text
Morgan fingerprint + RDKit descriptors -> multi-output ridge regression -> gene response vector
```

Then improve with:

- PCA/NMF latent response programs.
- MLP or chemical-embedding model.
- Ensembles for uncertainty.
- Pathway/gene-program analyses for interpretability.
- Scaffold or chemical-cluster validation splits.

The key early risk is leakage. Do not randomly split wells if the same compound
can appear in both train and validation. Split by compound, and keep duplicate
or near-duplicate chemical structures in the same split when possible.

## Files Added In This Branch

- `examples/control_drug_pca.py`: reusable script for control-drug PCA QC.
- `docs/control_drug_pca_eda.md`: detailed PCA EDA writeup and reproduction command.
- `docs/drug_seq_project_notes.md`: team-facing summary of dataset structure,
  QC findings, and modeling recommendations.
