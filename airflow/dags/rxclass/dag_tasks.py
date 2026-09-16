from airflow.decorators import task
import pandas as pd
from sagerx import get_rxcuis, load_df_to_pg, get_concurrent_api_results, write_json_lines, iter_json_lines, create_path
from common_dag_tasks import get_data_folder
import logging

def create_url_list(rxcui_list:list)-> list:
    urls=[]

    for rxcui in rxcui_list:
        urls.append(f"https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json?rxcui={rxcui}")
    return urls

@task
def extract(dag_id:str) -> str:
    """
    Retrieves RxClass concepts from RxNav for the EPC class type,
    processes them concurrently, and loads results into Postgres.
    """
    logging.info("Starting data retrieval for RxClass...")

    # 1. Fetch the list of concepts
    tty_list = ['IN','PIN','MIN','SCDC','SCDF','SCDFP','SCDG','SCDGP','SCD','GPCK','BN','SBDC','SBDF','SBDFP','SBDG','SBD','BPCK']
    #tty_list = ['SCD', 'SBD', 'GPCK', 'BPCK']
    #tty_list = ['BPCK']
    rxcui_list = get_rxcuis(tty_list, active_only = True)
    logging.info(f"Fetched {len(rxcui_list)} RXCUIs.")

    # 1.5. Create list of urls
    url_list = create_url_list(rxcui_list)

    results = get_concurrent_api_results(url_list)

    data_folder = get_data_folder(dag_id)
    # JSON LINES, not one array. The array form was 883 MB and `load` was
    # OOM-killed reading it back — see the note on write_json_lines. The format
    # is internal to this DAG: extract writes it, load reads it, nothing else
    # looks at the file.
    file_path = create_path(data_folder) / 'data.jsonl'
    file_path_str = file_path.resolve().as_posix()

    write_json_lines(file_path_str, results)

    print(f"Extraction Completed! Data saved to file: {file_path_str}")

    return file_path_str


COLUMNS = [
    "rxcui", "name", "tty", "rela",
    "class_id", "class_name", "class_type", "rela_source",
]

@task
def load(file_path_str:str):
    """
    Stream the extract and de-duplicate WHILE parsing.

    ⚠ Measured on the real 2026-09-15 extract, inside the airflow container:

        holding every row, then drop_duplicates()   3,697,857 rows   4,077 MB peak
        de-duplicating into a set as we go            130,121 rows     104 MB peak

    Same 130,121 rows out. 96.5% of the parsed rows are duplicates, so the list
    was ~28x larger than the result it produced.

    Switching the read to `iter_json_lines` removed the ~900 MB document parse
    that got this task OOM-killed (anon-rss 6.0 GB on 2026-09-08, 3.3 GB and
    6.1 GB on 2026-09-15). It did not remove this: the box has 7.8 GB, no swap,
    and roughly 2 GB already in use by postgres, dbt and the airflow services,
    so a 4 GB peak still lands inside the range the kernel killed us in. It
    would survive a quiet box and die during a concurrent dbt run, which is the
    worst kind of fixed.

    A tuple is hashable, so the set de-duplicates exactly as `drop_duplicates()`
    did, without ever materialising the duplicates.
    """
    seen = set()
    scanned = 0
    for result in iter_json_lines(file_path_str):
        # skip a result if it is None
        if result is None:
            continue
        scanned += 1
        response = result['response']
        if 'rxclassDrugInfoList' in response:
            for drug_info in response["rxclassDrugInfoList"]["rxclassDrugInfo"]:
                seen.add((
                    drug_info["minConcept"].get("rxcui"),
                    drug_info["minConcept"].get("name",""),
                    drug_info["minConcept"].get("tty",""),
                    drug_info.get("rela",""),
                    drug_info["rxclassMinConceptItem"].get("classId",""),
                    drug_info["rxclassMinConceptItem"].get("className",""),
                    drug_info["rxclassMinConceptItem"].get("classType",""),
                    drug_info.get("relaSource",""),
                ))

    df = pd.DataFrame(list(seen), columns=COLUMNS)
    print(f'Dataframe created of {len(df)} length, from {scanned} API results.')

    # ⚠ An empty frame would REPLACE the table with nothing. A run that parsed
    # no rows is a failure, not an instruction to empty the lake. The previous
    # shape could not hit this because it crashed instead; this one returns
    # cleanly from a bad extract, so it has to be said out loud.
    if df.empty:
        raise ValueError(
            f"rxclass parsed {scanned} API results and produced 0 rows; "
            "refusing to replace sagerx_lake.rxclass with an empty table"
        )

    load_df_to_pg(df,"sagerx_lake","rxclass","replace",index=False)
