"""Find Flywheel imaging data for a list of CBTN subjects.

Examples:
	python find_fw_data.py --subjects-csv madsen/sub_list.csv --output-csv madsen/cbtn_selected_fw.csv
	python find_fw_data.py --input-mode cbtn-all --diagnosis-filter "High-Grade Glioma" --output-csv madsen/cbtn_selected_fw.csv
"""

import argparse
import os
import sys

import flywheel
import pandas as pd

from utils import find_fw_data


def parse_args():
	parser = argparse.ArgumentParser(description=__doc__)
	parser.add_argument(
		"--source",
		choices=["d3b_warehouse", "flywheel"],
		default="d3b_warehouse",
		help="Data source used by utils.find_fw_data (default: d3b_warehouse).",
	)
	parser.add_argument(
		"--copy-level",
		choices=["subject", "session"],
		default="session",
		help="Whether to search/output at subject or session level (default: session).",
	)
	parser.add_argument(
		"--input-mode",
		choices=["subjects-csv", "cbtn-all"],
		default="subjects-csv",
		help="Input mode for building the subject list (default: subjects-csv).",
	)
	parser.add_argument(
		"--subjects-csv",
		default=None,
		help="CSV with a `CBTN Subject ID` column (required when --input-mode subjects-csv).",
	)
	parser.add_argument(
		"--output-csv",
		required=True,
		help="Output CSV path for selected Flywheel rows.",
	)
	parser.add_argument(
		"--subject-id-column",
		default="CBTN Subject ID",
		help="Column name containing subject IDs (default: CBTN Subject ID).",
	)
	parser.add_argument(
		"--cbtn-all-csv",
		default=None,
		help="Path to CBTN-all CSV. If omitted in cbtn-all mode, uses env var cbtn_all_table.",
	)
	parser.add_argument(
		"--diagnosis-filter",
		default="High-Grade Glioma",
		help="Substring filter applied to `CNS Diagnosis Category` in cbtn-all mode.",
	)
	parser.add_argument(
		"--api-key",
		default=None,
		help="Flywheel API key. Defaults to FW_API_KEY environment variable.",
	)
	return parser.parse_args()


def load_subjects(args):
	if args.input_mode == "subjects-csv":
		if not args.subjects_csv:
			raise ValueError("--subjects-csv is required when --input-mode subjects-csv")

		sub_df = pd.read_csv(args.subjects_csv)
		if args.subject_id_column not in sub_df.columns:
			raise ValueError(
				f"Missing subject ID column '{args.subject_id_column}' in {args.subjects_csv}"
			)
		return sub_df[[args.subject_id_column]].drop_duplicates().reset_index(drop=True)

	cbtn_all_csv = args.cbtn_all_csv or os.getenv("cbtn_all_table")
	if not cbtn_all_csv:
		raise ValueError(
			"--cbtn-all-csv (or env var cbtn_all_table) is required when --input-mode cbtn-all"
		)

	cbtn_df = pd.read_csv(cbtn_all_csv)
	required_columns = {"CNS Diagnosis Category", args.subject_id_column}
	missing = [col for col in required_columns if col not in cbtn_df.columns]
	if missing:
		raise ValueError(f"Missing columns in {cbtn_all_csv}: {missing}")

	selected = cbtn_df[
		cbtn_df["CNS Diagnosis Category"].fillna("").str.contains(args.diagnosis_filter)
	]
	return selected[[args.subject_id_column]].drop_duplicates().reset_index(drop=True)


def main():
	args = parse_args()

	api_key = args.api_key or os.getenv("FW_API_KEY")
	if not api_key:
		raise RuntimeError("Flywheel API key is required. Set FW_API_KEY or pass --api-key.")

	sub_df = load_subjects(args)
	if args.subject_id_column != "CBTN Subject ID":
		sub_df = sub_df.rename(columns={args.subject_id_column: "CBTN Subject ID"})

	fw = flywheel.Client(api_key)
	print("Flywheel Instance:", fw.get_config().site.api_url)
	print(f"Subjects loaded: {len(sub_df)}")
	print(f"Source: {args.source} | copy level: {args.copy_level}")

	fw_data_df = find_fw_data(fw, args.source, sub_df, args.copy_level)

	output_dir = os.path.dirname(args.output_csv)
	if output_dir:
		os.makedirs(output_dir, exist_ok=True)
	fw_data_df.to_csv(args.output_csv, index=False)
	print(f"Wrote {len(fw_data_df)} rows to {args.output_csv}")

	return 0


if __name__ == "__main__":
	sys.exit(main())
