"""Sync the dbt warehouse to Google Cloud Storage + BigQuery.

DELIBERATELY PAUSED in the rxreport deployment — do not unpause it there without
reading this first. It is the only paused DAG, so its state looks like an
oversight; it is not. Two independent reasons, either alone sufficient:

  1. This deployment does not sync to GCP at all. There is no GCS bucket and no
     BigQuery dataset behind GCS_BUCKET / GCP_PROJECT / GCP_DATASET, so every
     downstream task here has nowhere to write. Unpausing buys a nightly red run
     and nothing else.
  2. `run_dbt` below runs `dbt run` UNSCOPED — every model, not this DAG's own.
     That makes it fail on models belonging to other sources: as of 2026-08-12
     it dies creating stg_ashp__current_drug_shortages / _ndcs, because `ashp`
     cannot load (ashp.org is behind a Cloudflare challenge — see ashp/dag.py).
     So its failure is mostly a REPORT ON ANOTHER DAG, which makes it a bad
     signal to leave running: it would stay red after the GCP question was
     settled, for reasons that have nothing to do with GCP.

Revisit only if this deployment actually adopts BigQuery. Deleting it outright
is a separate upstream decision (it is inherited from coderxio/sagerx).
"""
import pendulum

from airflow_operator import create_dag, DEFAULT_START_DATE

from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python_operator import PythonOperator

from common_dag_tasks import run_subprocess_command

from os import environ

dag_id = "dbt_gcp"

dag = create_dag(
    dag_id=dag_id,
    schedule="0 4 * * *",
    start_date=DEFAULT_START_DATE,
    catchup=False,
    concurrency=2,
)

with dag:
    def run_dbt():
        run_subprocess_command(command=['docker','exec','dbt','dbt', 'run'], cwd='/dbt/sagerx', success_code=1)
        run_subprocess_command(command=['docker','exec','dbt','dbt', 'run-operation', 'check_data_availability'], cwd='/dbt/sagerx')

    def get_dbt_models():
        from sagerx import run_query_to_df

        df = run_query_to_df("""
            SELECT * 
            FROM sagerx.data_availability
            WHERE has_data =  True
            AND materialized != 'view'
            """)

        print(f"Final list: {df}")
        return df.to_dict('index')
    
    def load_to_gcs(run_dbt_task):
        
        dbt_models = get_dbt_models()
        gcp_tasks = []
        for _,row_values in dbt_models.items():
            schema_name = row_values.get('schema_name')
            table_name =  row_values.get('table_name')
            columns_info = row_values.get('columns_info')

            print(f"{schema_name}.{table_name}")
            print(columns_info)

            # Transfer data from Postgres DB to Google Cloud Storage DB
            p2g_task = PostgresToGCSOperator(
                task_id=f'postgres_to_gcs_{schema_name}.{table_name}',
                postgres_conn_id='postgres_default',  
                sql=f"SELECT * FROM {schema_name}.{table_name}",
                bucket=environ.get("GCS_BUCKET"), 
                filename=f'{table_name}', 
                export_format='csv', 
                gzip=False,  
                dag=dag,
            )

            # Populate BigQuery tables with data from Cloud Storage Bucket
            cs2bq_task = GCSToBigQueryOperator(
                task_id=f"bq_load_{schema_name}.{table_name}",
                bucket=environ.get("GCS_BUCKET"),
                source_objects=[f"{table_name}"],
                destination_project_dataset_table=f"{environ.get('GCP_PROJECT')}.{environ.get('GCP_DATASET')}.{table_name}",
                autodetect = True,
                external_table=False,
                create_disposition= "CREATE_IF_NEEDED", 
                write_disposition= "WRITE_TRUNCATE", 
                gcp_conn_id = 'google_cloud_default',
                dag = dag,
            )


            run_dbt_task.set_downstream(p2g_task)
            p2g_task.set_downstream(cs2bq_task)

            gcp_tasks.append(p2g_task)
            gcp_tasks.append(cs2bq_task)

        return gcp_tasks


    run_dbt_task = PythonOperator(
        task_id='run_dbt',
        python_callable=run_dbt,
        dag=dag,
    )

    gcp_tasks = load_to_gcs(run_dbt_task)


"""
Notes:
- Scaling issue if each table contains multiple tasks, can quickly grow since it grows exponentially 
- Single dbt run is the preferred method since the complexity of dependencies is handled once 
- If we can have dbt full run after each dag run and then only specify which tables to upload that would make it more targeted and less costly 
"""
