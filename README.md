# Scripts to use smart-copy functionality on Flywheel

## Overview

This repository contains scripts to perform soft-copy operations on Flywheel, enabling efficient data management across projects (for a given subject cohort, create a project that contains soft links to all source data without hard-copying). 
The workflow consists of two main steps:

1. **Discovery**: Locate matching Flywheel data based on subject identifiers
2. **Copy**: Perform soft-copy operations to a destination project

## Prerequisites

- Python 3.x
- Flywheel SDK
- Valid Flywheel API credentials
- (Optional but likely required) Access to D3b warehouse for source data queries

## Quick Start

```bash
# Step 1: Find and catalog subjects
python find_fw_data.py --subjects-csv subjects.csv --output-csv fw_data.csv

# Step 2: Soft-copy to destination
python fw_soft_copy_sdk.py --input-csv fw_data.csv --destination-project group/project --yes
```

## Main Usage

### Create CSV of subject IDs

CSV Format: A single column `CBTN Subject ID` with the list of subjects to include in the cohort (C-IDs)

### Step 1

`find_fw_data.py` is a command-line script. It takes subject input (CSV), finds matching Flywheel data, and writes a CSV with project/subject/session info.

Required environment variable:

- **FW_API_KEY** : user's Flywheel API key (unless passed with `--api-key`)

Optional environment variable:

- **cbtn_all_table** : path to CBTN-all CSV (used when `--input-mode cbtn-all` and `--cbtn-all-csv` is not passed)

If using D3b warehouse as source (`--source d3b_warehouse`), also set:

- **DB_URL** : D3b warehouse connection URL
- **DB_USER** : warehouse username
- **DB_PASSWORD** : warehouse password

#### Command usage

```bash
python find_fw_data.py --subjects-csv gfac/sub_list.csv --output-csv gfac/cbtn_selected_fw.csv
```

```bash
python find_fw_data.py --input-mode cbtn-all --diagnosis-filter "High-Grade Glioma" --output-csv gfac/cbtn_selected_fw.csv
```

Main arguments:

- `--source {d3b_warehouse,flywheel}` (default: `d3b_warehouse`)
- `--copy-level {subject,session}` (default: `session`)
- `--input-mode {subjects-csv,cbtn-all}` (default: `subjects-csv`)
- `--subjects-csv` path to CSV containing `CBTN Subject ID`
- `--output-csv` output file path (required)
- `--subject-id-column` custom subject ID column name (default: `CBTN Subject ID`)
- `--cbtn-all-csv` optional path override for CBTN-all table
- `--diagnosis-filter` text filter on `CNS Diagnosis Category` in cbtn-all mode

### Step 2

`fw_soft_copy_sdk.py` is a command-line script that takes the CSV output from Step 1 and runs Flywheel soft-copy operations into a destination project.

Behavior:

- Copies at the **session** level when `Session` column exists (or `--copy-level session` is set).
- Copies at the **subject** level when `Session` is absent (or `--copy-level subject` is set).
- Skips rows that already exist in the destination project.

#### Command usage

```bash
python fw_soft_copy_sdk.py --input-csv gfac/cbtn_selected_fw.csv --destination-project gfac/your_destination_project --source-group d3b --yes
```

Dry-run first:

```bash
python fw_soft_copy_sdk.py --input-csv gfac/cbtn_selected_fw.csv --destination-project gfac/your_destination_project --source-group d3b --dry-run --yes
```

Main arguments:

- `--input-csv` input CSV path (required)
- `--destination-project` destination path in form `<group>/<project_label>` (required)
- `--source-group` source group label used for lookups (default: `d3b`)
- `--copy-level {auto,subject,session}` (default: `auto`)
- `--project-column` (default: `Project`)
- `--subject-column` (default: `CBTN Subject ID`)
- `--session-column` (default: `Session`)
- `--dry-run` print planned copies without launching them
- `--yes` skip interactive confirmation prompt
- `--strict` stop on first copy error
- `--api-key` (default: `FW_API_KEY` environment variable) Flywheel API key
