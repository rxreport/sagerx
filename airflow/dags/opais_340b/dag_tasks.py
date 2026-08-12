import logging
import pandas as pd
from airflow.decorators import task
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from sagerx import load_df_to_pg, free_text_to_snake


logger = logging.getLogger(__name__)


def _clean_sheet(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned.columns = [free_text_to_snake(col) for col in cleaned.columns]
    cleaned = cleaned.loc[:, [col for col in cleaned.columns if col and not col.startswith("unnamed")]]
    cleaned = cleaned.dropna(how="all")
    return cleaned.reset_index(drop=True)


MANUAL_DROP_HELP = """\
This DAG has NO extract task by design — it loads a workbook that a human has to
put in place first. That is not an oversight: HRSA states in its own OPAIS FAQ
that "there is no separate API / webservice for programmatic access by external
vendors at this time", direct HTTP requests are unsupported, and the download
button needs session + viewstate context (vendors are told to drive it with
Selenium). So there is nothing for an extract task to call.

To run this DAG:
  1. Download "Covered Entity Daily Report" (Excel) from
     https://340bopais.hrsa.gov/Reports  — it is regenerated daily just after
     midnight Eastern.
  2. Drop the .xlsx in {data_folder} (inside the airflow containers; on the prod
     host that path is bind-mounted from
     ./apps/sagerx/airflow/data/opais_340b/). Create the directory if needed.
  3. Trigger the DAG manually — `schedule=None` is correct and deliberate,
     because a schedule would only produce a daily failure on a file nobody
     placed. Its `next_dagrun` being NULL is that setting, not a broken DAG.

The workbook must contain these three worksheets, with the header on row 4:
{sheets}"""


def _drop_help(data_folder: Path, sheets) -> str:
    return MANUAL_DROP_HELP.format(
        data_folder=data_folder,
        sheets="\n".join("  - {}".format(s) for s in sheets),
    )


def _get_latest_excel(data_folder: Path, sheets=()) -> Path:
    if not data_folder.is_dir():
        raise FileNotFoundError(
            "OPAIS data folder {} does not exist.\n\n{}".format(
                data_folder, _drop_help(data_folder, sheets)
            )
        )

    excel_files = list(data_folder.glob("*.xlsx"))

    if not excel_files:
        other = sorted(p.name for p in data_folder.iterdir() if p.is_file())
        raise FileNotFoundError(
            "No .xlsx workbook found in {} (directory holds: {}).\n\n{}".format(
                data_folder,
                ", ".join(other) if other else "nothing",
                _drop_help(data_folder, sheets),
            )
        )

    return max(excel_files, key=lambda p: p.stat().st_mtime)


def _load_cleaned_sheet(sheet_name: str, raw_df: pd.DataFrame, table: str) -> None:
    logger.info("Cleaning sheet '%s' (%d rows)", sheet_name, len(raw_df))
    df = _clean_sheet(raw_df)
    rows = len(df)
    logger.info("Sheet '%s' cleaned to %d rows", sheet_name, rows)

    if df.empty:
        logger.warning("Sheet '%s' is empty after cleaning. Skipping %s.", sheet_name, table)
        return

    load_df_to_pg(df, "sagerx_lake", table, "replace", index=False)
    logger.info("Loaded %d rows from sheet '%s' into %s", rows, sheet_name, table)


@task
def load(dag_id: str, data_folder: str) -> None:
    sheet_to_table = {
        "Covered Entities": "opais_340b_covered_entities",
        "Shipping Addresses": "opais_340b_shipping_addresses",
        "Contract Pharmacies": "opais_340b_contract_pharmacies",
    }

    latest_file = _get_latest_excel(Path(data_folder), sheet_to_table.keys())
    logger.info("Starting OPAIS load from file: %s", latest_file)
    sheet_names = list(sheet_to_table.keys())
    logger.info("Reading workbook with sheets: %s", ", ".join(sheet_names))
    sheet_frames = pd.read_excel(
        latest_file,
        sheet_name=sheet_names,
        header=3,
        dtype=str,
        engine="openpyxl",
    )
    logger.info("Completed workbook read")

    max_workers = min(3, len(sheet_to_table))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_load_cleaned_sheet, sheet, sheet_frames[sheet], table): (sheet, table)
            for sheet, table in sheet_to_table.items()
        }

        for future in as_completed(future_map):
            try:
                future.result()
            except Exception:
                sheet, table = future_map[future]
                logger.exception("Sheet '%s' failed to load into %s", sheet, table)
                raise

    logger.info("Completed OPAIS load")
