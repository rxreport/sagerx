import json
import logging
import os
import re
from datetime import date, datetime
from time import sleep

import requests
import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd

import pendulum

from airflow_operator import create_dag, DEFAULT_START_DATE
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.providers.postgres.operators.postgres import PostgresOperator

from common_dag_tasks import  extract, transform, generate_sql_list, get_ds_folder
from sagerx import read_sql_file, load_df_to_pg

from airflow.decorators import task


# ---------------------------------------------------------------------------
# KNOWN BROKEN — the upstream source is gated, not the code below.
#
# ashp.org sits behind Cloudflare's managed challenge (the "Just a moment..."
# JS/Turnstile interstitial). Re-verified 2026-08-12, and the picture is worse
# than the 2026-04-25 note it replaces:
#   * The block is SITE-WIDE, not specific to the shortages path or to our
#     egress. https://www.ashp.org/ itself, the shortages list, and a detail
#     page all answer 403 + "Just a moment..." — from the prod Linode AND from
#     an unrelated residential IP. So an egress proxy alone would not fix it.
#   * cloudscraper 1.2.71 (installed; also the newest release that exists —
#     the project has shipped nothing since 2023) returns 403 from inside the
#     airflow container. Bumping the pin cannot help: cloudscraper solves the
#     LEGACY IUAM JS challenge, and this is the current managed challenge.
# Previously tested and also failed: curl_cffi TLS impersonation (chrome110/
# 119/120), vanilla Playwright + headless Chromium 115, playwright-stealth 1.x.
#
# Deliberately NOT attempted: solving the challenge. Defeating a CAPTCHA/bot
# gate is out of bounds for this pipeline, so this DAG fails loudly and stays
# red rather than pretending. Real ways out, in preference order:
#   1. Ask ASHP for a data-sharing/API arrangement (they publish this as a
#      public-health resource; a licensed feed is the durable answer).
#   2. Switch the source to FDA's drug-shortage database, which is open and
#      documented. NOTE: it is a DIFFERENT dataset with different columns, so
#      it is a new DAG plus new dbt staging models, not a URL swap.
#   3. A real (non-headless) browser runner on a newer base image.
# Until one of those lands, `ashp` failing daily is expected and truthful, and
# `dbt_gcp` stays paused because its models read this DAG's tables.
# ---------------------------------------------------------------------------
dag_id = "ashp"

dag = create_dag(
    dag_id=dag_id,
    schedule="0 4 * * *",
    start_date=DEFAULT_START_DATE,
    catchup=False,
    concurrency=2,
)

with dag:
    landing_url = "https://www.ashp.org/drug-shortages/current-shortages/drug-shortages-list?page=CurrentShortages"
    base_url = "https://www.ashp.org/drug-shortages/current-shortages/"
    ndc_regex = re.compile(r"\d{5}\-\d{4}\-\d{2}")  # ASHP shortage pages always have 5-4-2 format NDCs
    created_regex = re.compile(r"Created (\w+ \d+, \d+)")
    updated_regex = re.compile(r"Updated (\w+ \d+, \d+)")

    ds_folder = get_ds_folder(dag_id)

    transform_task = transform(dag_id)

    @task
    def extract_load_shortage_list():
        logging.basicConfig(level=logging.INFO, format='%(asctime)s : %(levelname)s : %(message)s')

        # Use cloudscraper to bypass Cloudflare protection
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            }
        )
        
        logging.info('Checking ASHP website for updates')
        shortage_list = scraper.get(landing_url)

        if shortage_list.status_code != 200:
            # Log a BOUNDED snippet. This used to log `shortage_list.text` in
            # full, which meant a ~6 KB Cloudflare challenge page was written to
            # the task log at ERROR every single day for months.
            snippet = (shortage_list.text or '')[:300].replace('\n', ' ')
            challenged = (
                shortage_list.status_code == 403
                and 'just a moment' in (shortage_list.text or '').lower()
            )
            if challenged:
                # Do not retry: the challenge is deterministic, and the default
                # `retries: 1` otherwise burns a second attempt every run to
                # reach the identical wall. AirflowFailException skips retries.
                raise AirflowFailException(
                    'ASHP is behind a Cloudflare managed challenge — HTTP {} from {} '
                    '(cloudscraper {} cannot solve it; see the note at the top of this '
                    'DAG for why, and for the three real ways out). This is an UPSTREAM '
                    'ACCESS BLOCK, not a bug in this DAG and not a transient outage. '
                    'Response began: {}'.format(
                        shortage_list.status_code,
                        landing_url,
                        getattr(cloudscraper, '__version__', 'unknown'),
                        snippet,
                    )
                )
            raise AirflowException(
                'ASHP website unreachable — HTTP {} from {}. Response began: {}'.format(
                    shortage_list.status_code, landing_url, snippet
                )
            )

        ashp_drugs = []
        soup = BeautifulSoup(shortage_list.content, 'html.parser')
        for link in soup.find(id='1_dsGridView').find_all('a'):
            ashp_drugs.append({
                'name': link.get_text(),
                'detail_url': link.get('href')
            })

        affected_ndcs = []
        available_ndcs = []

        for shortage in ashp_drugs:
            shortage_detail_data = scraper.get(base_url + shortage['detail_url'])
            soup = BeautifulSoup(shortage_detail_data.content, 'html.parser')

            # Get shortage reasons
            shortage_reasons = []
            try:
                for reason in soup.find(id='1_lblReason').find_all('li'):
                    shortage_reasons.append(reason.get_text())
            except AttributeError:
                logging.debug(f'No shortage reasons for {shortage.get("name")}')
                shortage['shortage_reasons'] = None
            else:
                shortage['shortage_reasons'] = json.dumps(shortage_reasons)

            # Get resupply dates
            resupply_dates = []
            try:
                for date_info in soup.find(id='1_lblResupply').find_all('li'):
                    resupply_dates.append(date_info.get_text())
            except AttributeError:
                logging.debug(f'No resupply dates for {shortage.get("name")}')
                shortage['resupply_dates'] = None
            else:
                shortage['resupply_dates'] = json.dumps(resupply_dates)

            # Get implications on patient care
            care_implications = []
            try:
                for implication in soup.find(id='1_lblImplications').find_all('li'):
                    care_implications.append(implication.get_text())
            except AttributeError:
                logging.debug(f'No care implications for {shortage.get("name")}')
                shortage['care_implications'] = None
            else:
                shortage['care_implications'] = json.dumps(care_implications)

            # Get safety information
            safety_notices = []
            try:
                for notice in soup.find(id='1_lblSafety').find_all('li'):
                    safety_notices.append(notice.get_text())
            except AttributeError:
                logging.debug(f'No safety notices for {shortage.get("name")}')
                shortage['safety_notices'] = None
            else:
                shortage['safety_notices'] = json.dumps(safety_notices)

            # Get alternative agents and management info
            alternatives = []
            try:
                for alternative in soup.find(id='1_lblAlternatives').find_all('li'):
                    alternatives.append(alternative.get_text())
            except AttributeError:
                logging.debug(f'No alternatives/management information for {shortage.get("name")}')
                shortage['alternatives_and_management'] = None
            else:
                shortage['alternatives_and_management'] = json.dumps(alternatives)

            # Get affected NDCs
            try:
                for ndc_description in soup.find(id='1_lblProducts').find_all('li'):
                    ndc_data = {
                        'detail_url': shortage['detail_url'],
                        'ndc_description': ndc_description.get_text(),
                    }
                    if ',' in ndc_data['ndc_description']:
                        affected_ndcs.append(ndc_data)
            except (TypeError, AttributeError):
                logging.debug(f'No affected NDCs for {shortage.get("name")}')

            # Get currently available NDCs
            try:
                for ndc_description in soup.find(id='1_lblAvailable').find_all('li'):
                    ndc_data = {
                        'detail_url': shortage['detail_url'],
                        'ndc_description': ndc_description.get_text(),
                    }
                    if ',' in ndc_data['ndc_description']:
                        available_ndcs.append(ndc_data)
            except (TypeError, AttributeError):
                logging.debug(f'No available NDCs for {shortage.get("name")}')

            # Get created date
            stamp = soup.find(id='1_lblUpdated').find('p').get_text()
            try:
                created_date = created_regex.search(stamp).group(1)
                created_date = datetime.strptime(created_date, '%B %d, %Y')
                shortage['created_date'] = created_date
            except AttributeError:
                logging.debug(f'Missing ASHP created date for {shortage.get("name")}')
                shortage['created_date'] = None
            except ValueError:
                logging.error(f'Could not parse created date for {shortage.get("name")}')
                shortage['created_date'] = None

            # Get last updated date
            try:
                updated_date = updated_regex.search(stamp).group(1)
                updated_date = datetime.strptime(updated_date, '%B %d, %Y')
                shortage['updated_date'] = updated_date
            except AttributeError:
                logging.debug(f'Missing ASHP update date for {shortage.get("name")}')
                shortage['updated_date'] = None
            except ValueError:
                logging.error(f'Could not parse update date for {shortage.get("name")}')
                shortage['updated_date'] = None

            sleep(0.2)
        
        if len(ashp_drugs) > 0:
            # Load the main shortage table
            shortage_columns = ['name', 'detail_url', 'shortage_reasons', 'resupply_dates',
                                'alternatives_and_management', 'care_implications', 'safety_notices',
                                'created_date', 'updated_date']
            shortages = pd.DataFrame(ashp_drugs, columns=shortage_columns)
            load_df_to_pg(shortages, "sagerx_lake", "ashp_shortage_list", "replace", index=False)

            # Load the table of affected and available NDCs
            affected_ndcs = pd.json_normalize(affected_ndcs)
            affected_ndcs['ndc_type'] = 'affected'
            available_ndcs = pd.json_normalize(available_ndcs)
            available_ndcs['ndc_type'] = 'available'

            ndcs = pd.concat([affected_ndcs, available_ndcs])
            ndcs = ndcs[~ndcs['ndc_description'].isnull()]  # Remove shortages that have no associated NDCs
            load_df_to_pg(ndcs, "sagerx_lake", "ashp_shortage_list_ndcs", "replace", index=False)
        else:
            # Previously this only logged an error and let the task SUCCEED, so a
            # page that fetched but parsed to nothing looked like a good run and
            # silently left the warehouse holding whatever it held before.
            raise AirflowException(
                'ASHP returned HTTP 200 but no shortages parsed out of {} — the page '
                'markup has probably changed (the parser keys off the ASP.NET id '
                '"1_dsGridView"). Refusing to report success on an empty '
                'extract.'.format(landing_url)
            )


    extract_load_shortage_list() >> transform_task
