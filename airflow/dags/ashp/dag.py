import json
import logging
import os
import re
from datetime import date, datetime
from time import sleep

import requests
from bs4 import BeautifulSoup
import pandas as pd

import pendulum

from airflow_operator import create_dag
from airflow.providers.postgres.operators.postgres import PostgresOperator

from common_dag_tasks import  extract, transform, generate_sql_list, get_ds_folder
from sagerx import read_sql_file, load_df_to_pg

from airflow.decorators import task


dag_id = "ashp"

dag = create_dag(
    dag_id=dag_id,
    schedule="0 4 * * *",
    start_date=pendulum.yesterday(),
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

        # Playwright is needed because ASHP sits behind Cloudflare's managed
        # challenge — the "Just a moment..." JS check that no TLS-only client
        # (cloudscraper, curl_cffi) can solve. Headless Chromium executes the
        # JS, gets the cf_clearance cookie, then we reuse the BrowserContext
        # across the landing page → ~50 detail pages so the cookie carries.
        # Lazy-import here so other DAGs don't pay the import cost at parse time.
        from playwright.sync_api import sync_playwright
        # playwright-stealth patches the page so navigator properties, WebGL,
        # plugin enumeration, and Permission API match a real Chrome — without
        # this, vanilla Playwright still gets caught by Cloudflare's managed
        # challenge fingerprinting. Wrap each page after creation.
        from playwright_stealth import stealth_sync

        affected_ndcs = []
        available_ndcs = []
        ashp_drugs = []

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ],
            )
            context = browser.new_context(
                # Match the bundled Chromium version (115) — UA mismatched with
                # TLS fingerprint is itself a bot-detection tell.
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/115.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
                locale="en-US",
            )
            # Hide the navigator.webdriver flag that headless Chromium sets
            # by default — Cloudflare's bot detection looks at this.
            context.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
            )
            page = context.new_page()
            # Apply playwright-stealth's full set of patches on top of the
            # webdriver hide above. Order matters — stealth_sync must run
            # before the first page.goto().
            stealth_sync(page)

            logging.info('Checking ASHP website for updates')
            page.goto(landing_url, wait_until="networkidle", timeout=60_000)
            landing_html = page.content()

            if "Just a moment" in landing_html or "challenges.cloudflare.com" in landing_html:
                logging.error('ASHP landing still showing Cloudflare challenge after networkidle')
                logging.error(f'Body head: {landing_html[:500]}')
                raise RuntimeError(
                    "Playwright could not pass Cloudflare's challenge for ASHP landing — "
                    "may need playwright-extra / rebrowser-playwright stealth patches"
                )

            soup = BeautifulSoup(landing_html, 'html.parser')
            grid = soup.find(id='1_dsGridView')
            if grid is None:
                logging.error(f'Landing did not contain id="1_dsGridView". Body head: {landing_html[:500]}')
                raise RuntimeError("ASHP landing layout changed — id='1_dsGridView' not found")
            for link in grid.find_all('a'):
                ashp_drugs.append({
                    'name': link.get_text(),
                    'detail_url': link.get('href')
                })

            for shortage in ashp_drugs:
                page.goto(base_url + shortage['detail_url'], wait_until="networkidle", timeout=60_000)
                detail_html = page.content()
                soup = BeautifulSoup(detail_html, 'html.parser')

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

            context.close()
            browser.close()

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
            logging.error('Drug shortage list not found')
        
    extract_load_shortage_list() >> transform_task
