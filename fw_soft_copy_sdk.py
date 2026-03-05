## Soft copy subjects/sessions from various projects into a single Flywheel project
#   This script uses the Flywheel SDK to perform a "soft copy" of subjects/sessions
#   from various source projects into a single destination project. The script checks
#   if the subject/session already exists in the destination project before copying,
#   and skips if it does. The level of copying (subject vs session) is determined by the structure of the input CSV file.
#
#   INPUT CSV: should have columns for 'Project', 'CBTN Subject ID', and optionally 'Session' (if copying at session level)
#   - if 'Session' column is present, the script will copy at the session level
#   - if 'Session' column is not present, the script will copy at the subject level
#
#   Note: the script uses the Flywheel SDK's subject_copy and session_copy functions,
#   which performs a "soft copy" (i.e. the files are not duplicated, but the same files are
#   referenced in the new location). This is more efficient than downloading and re-uploading files
#
#   Example usage:
#       python fw_soft_copy_sdk.py --input-csv input.csv --destination-project group/destination_project

import argparse
import os
import sys

import flywheel
import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-csv",
        required=True,
        help="Input CSV with columns: Project, CBTN Subject ID, and optionally Session.",
    )
    parser.add_argument(
        "--destination-project",
        required=True,
        help="Destination Flywheel project path in form <group>/<project_label>.",
    )
    parser.add_argument(
        "--source-group",
        default="d3b",
        help="Source Flywheel group label used for source lookups (default: d3b).",
    )
    parser.add_argument(
        "--copy-level",
        choices=["auto", "subject", "session"],
        default="auto",
        help="Copy level. auto infers from whether Session column exists (default: auto).",
    )
    parser.add_argument(
        "--subject-column",
        default="CBTN Subject ID",
        help="Subject ID column in input CSV (default: CBTN Subject ID).",
    )
    parser.add_argument(
        "--project-column",
        default="Project",
        help="Project column in input CSV (default: Project).",
    )
    parser.add_argument(
        "--session-column",
        default="Session",
        help="Session column in input CSV (default: Session).",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="Flywheel API key. Defaults to FW_API_KEY environment variable.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Run non-interactively without the Enter confirmation prompt.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Stop on first copy error.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned actions without launching copy operations.",
    )
    return parser.parse_args()


def soft_copy_subject(fw, subject_id, sub_label, dest_proj):
    body = flywheel.models.SubjectCopyInput(dst_project_id=dest_proj.id, filter={})
    fw.subject_copy(subject_id, body)


def soft_copy_session(fw, sub_label, session_label, session_id, dest_proj):
    dest_subject = dest_proj.subjects.find_first(f"label={sub_label}")
    if dest_subject is None:
        dest_subject = dest_proj.add_subject(label=sub_label)

    body = flywheel.models.SessionCopyInput(
        dst_session_label=session_label,
        dst_project_id=dest_proj.id,
        filter={},
    )
    fw.session_copy(session_id, body)


def get_copy_level(sub_df, requested_level, session_column):
    if requested_level in {"subject", "session"}:
        return requested_level
    return "session" if session_column in sub_df.columns else "subject"


def build_existing_dataframe(fw, dest_proj, copy_level):
    view = fw.View(
        container=copy_level,
        match="all",
        filename="*",
        include_ids=False,
        include_labels=True,
        process_files=False,
        sort=False,
    )
    flywheel_df = fw.read_view_dataframe(view, dest_proj.id)
    if "subject.label" not in flywheel_df.columns:
        flywheel_df["subject.label"] = None
    if "session.label" not in flywheel_df.columns:
        flywheel_df["session.label"] = None
    return flywheel_df[["subject.label", "session.label"]].drop_duplicates().reset_index(drop=True)


def main():
    args = parse_args()

    api_key = args.api_key or os.getenv("FW_API_KEY")
    if not api_key:
        raise RuntimeError("Flywheel API key is required. Set FW_API_KEY or pass --api-key.")

    sub_df = pd.read_csv(args.input_csv)
    required = {args.project_column, args.subject_column}
    missing_required = [col for col in required if col not in sub_df.columns]
    if missing_required:
        raise ValueError(f"Missing required columns in {args.input_csv}: {missing_required}")

    fw = flywheel.Client(api_key)
    print("Flywheel Instance:", fw.get_config().site.api_url)

    copy_level = get_copy_level(sub_df, args.copy_level, args.session_column)
    if copy_level == "session" and args.session_column not in sub_df.columns:
        raise ValueError(
            f"--copy-level session requires '{args.session_column}' column in input CSV"
        )

    print(f"Destination project: {args.destination_project}")
    print(f"Source group: {args.source_group}")
    print(f"Copy level: {copy_level}")
    print(f"Rows in input CSV: {len(sub_df)}")

    if not args.yes:
        input("Press Enter to continue...")

    dest_proj = fw.lookup(args.destination_project)
    print(
        f"Pulling DataView of existing data in destination project ({args.destination_project})..."
    )
    existing_df = build_existing_dataframe(fw, dest_proj, copy_level)

    copied_count = 0
    skipped_count = 0
    error_count = 0
    n_rows = len(sub_df)

    for index, row in sub_df.iterrows():
        project_label = row[args.project_column]
        sub_label = row[args.subject_column]

        try:
            if copy_level == "session":
                session_label = row[args.session_column]
                exists = (
                    (existing_df["subject.label"] == sub_label)
                    & (existing_df["session.label"] == session_label)
                ).any()

                if exists:
                    skipped_count += 1
                    print(
                        f"{index + 1}/{n_rows}  SKIPPING session {session_label} for subject {sub_label} - already exists in destination project."
                    )
                    continue

                print(
                    f"{index + 1}/{n_rows}  Copying session {session_label} for subject {sub_label} from project {project_label}..."
                )
                if args.dry_run:
                    copied_count += 1
                    continue

                source_session = fw.lookup(
                    f"{args.source_group}/{project_label}/{sub_label}/{session_label}"
                )
                soft_copy_session(fw, sub_label, session_label, source_session.id, dest_proj)

            else:
                exists = (existing_df["subject.label"] == sub_label).any()
                if exists:
                    skipped_count += 1
                    print(
                        f"{index + 1}/{n_rows}  SKIPPING subject {sub_label} - already exists in destination project."
                    )
                    continue

                print(
                    f"{index + 1}/{n_rows}  Copying subject {sub_label} from project {project_label}..."
                )
                if args.dry_run:
                    copied_count += 1
                    continue

                source_subject = fw.lookup(f"{args.source_group}/{project_label}/{sub_label}")
                soft_copy_subject(fw, source_subject.id, sub_label, dest_proj)

            copied_count += 1

        except Exception as exc:
            error_count += 1
            print(f"{index + 1}/{n_rows}  ERROR: {exc}")
            if args.strict:
                raise

    print("--- summary ---")
    print(f"planned/copied: {copied_count}")
    print(f"skipped existing: {skipped_count}")
    print(f"errors: {error_count}")

    return 1 if error_count else 0


if __name__ == "__main__":
    sys.exit(main())
