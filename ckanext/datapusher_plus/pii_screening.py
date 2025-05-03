# encoding: utf-8
# flake8: noqa: E501

import json
import logging
import os
from pathlib import Path
import requests
import psycopg2
from psycopg2 import sql

import ckanext.datapusher_plus.utils as utils
import ckanext.datapusher_plus.config as conf
import ckanext.datapusher_plus.datastore_utils as dsu
from ckanext.datapusher_plus.qsv_utils import QSVCommand


def screen_for_pii(
    tmp: str,
    resource: dict,
    qsv: QSVCommand,
    temp_dir: str,
    logger: logging.Logger,
) -> bool:
    """
    Screen a file for Personally Identifiable Information (PII) using qsv's searchset command.

    Args:
        tmp: Path to the file to screen
        resource: Resource dictionary containing metadata
        qsv: QSVCommand instance
        temp_dir: Temporary directory path
        logger: Logger instance

    Returns:
        tuple[bool, float]: Tuple containing (pii_found, piiscreening_elapsed)
    """

    pii_found_abort = conf.PII_FOUND_ABORT

    # DP+ comes with default regex patterns for PII (SSN, credit cards,
    # email, bank account numbers, & phone number). The DP+ admin can
    # use a custom set of regex patterns by pointing to a resource with
    # a text file, with each line having a regex pattern, and an optional
    # label comment prefixed with "#" (e.g. #SSN, #Email, #Visa, etc.)
    if conf.PII_REGEX_RESOURCE_ID:
        pii_regex_resource_exist = dsu.datastore_resource_exists(
            conf.PII_REGEX_RESOURCE_ID
        )
        if pii_regex_resource_exist:
            pii_resource = dsu.get_resource(conf.PII_REGEX_RESOURCE_ID)
            pii_regex_url = pii_resource["url"]

            r = requests.get(pii_regex_url)
            pii_regex_file = pii_regex_url.split("/")[-1]

            p = Path(__file__).with_name("user-pii-regexes.txt")
            with p.open("wb") as f:
                f.write(r.content)
    else:
        pii_regex_file = "default-pii-regexes.txt"
        p = Path(__file__).with_name(pii_regex_file)

    pii_found = False
    pii_regex_fname = p.absolute()

    if conf.PII_QUICK_SCREEN:
        logger.info("Quickly scanning for PII using %s...", pii_regex_file)
        try:
            qsv_searchset = qsv.searchset(
                pii_regex_fname,
                tmp,
                ignore_case=True,
                quick=True,
            )
        except utils.JobError as e:
            raise utils.JobError("Cannot quickly search CSV for PII: %s", e)
        pii_candidate_row = str(qsv_searchset.stderr)
        if pii_candidate_row:
            pii_found = True

    else:
        logger.info("Scanning for PII using %s...", pii_regex_file)
        qsv_searchset_csv = os.path.join(temp_dir, "qsv_searchset.csv")
        try:
            qsv_searchset = qsv.searchset(
                pii_regex_file,
                tmp,
                ignore_case=True,
                flag="PII_info",
                flag_matches_only=True,
                json_output=True,
                output_file=qsv_searchset_csv,
            )
        except utils.JobError as e:
            raise utils.JobError("Cannot search CSV for PII: %s", e)
        pii_json = json.loads(str(qsv_searchset.stderr))
        pii_total_matches = int(pii_json["total_matches"])
        pii_rows_with_matches = int(pii_json["rows_with_matches"])
        if pii_total_matches > 0:
            pii_found = True

    if pii_found and pii_found_abort and not conf.PII_SHOW_CANDIDATES:
        logger.error("PII Candidate/s Found!")
        if conf.PII_QUICK_SCREEN:
            raise utils.JobError(
                "PII CANDIDATE FOUND on row %s! Job aborted.",
                pii_candidate_row.rstrip(),
            )
        else:
            raise utils.JobError(
                "PII CANDIDATE/S FOUND! Job aborted. Found %d PII candidate/s in %d row/s.",
                pii_total_matches,
                pii_rows_with_matches,
            )
    elif pii_found and conf.PII_SHOW_CANDIDATES and not conf.PII_QUICK_SCREEN:
        # TODO: Create PII Candidates resource and set package to private if its not private
        # ------------ Create PII Preview Resource ------------------
        logger.warning(
            "PII CANDIDATE/S FOUND! Found %d PII candidate/s in %d row/s. Creating PII preview...",
            pii_total_matches,
            pii_rows_with_matches,
        )
        pii_resource_id = resource["id"] + "-pii"

        try:
            raw_connection_pii = psycopg2.connect(conf.DATASTORE_WRITE_URL)
        except psycopg2.Error as e:
            raise utils.JobError("Could not connect to the Datastore: %s", e)
        else:
            cur_pii = raw_connection_pii.cursor()

        # check if the pii already exist
        existing_pii = dsu.datastore_resource_exists(pii_resource_id)

        # Delete existing pii preview before proceeding.
        if existing_pii:
            logger.info('Deleting existing PII preview "%s".', pii_resource_id)

            cur_pii.execute(
                "SELECT alias_of FROM _table_metadata where name like %s group by alias_of;",
                (pii_resource_id + "%",),
            )
            pii_alias_result = cur_pii.fetchone()
            if pii_alias_result:
                existing_pii_alias_of = pii_alias_result[0]

                dsu.delete_datastore_resource(existing_pii_alias_of)
                dsu.delete_resource(existing_pii_alias_of)

        pii_alias = [pii_resource_id]

        # run stats on pii preview CSV to get header names and infer data types
        # we don't need summary statistics, so use the --typesonly option
        try:
            qsv_pii_stats = qsv.stats(
                qsv_searchset_csv,
                infer_dates=False,
                dates_whitelist=conf.QSV_DATES_WHITELIST,
                stats_jsonl=False,
                prefer_dmy=False,
                cardinality=False,
                summary_stats_options=None,
                output_file=None,
            )
        except utils.JobError as e:
            raise utils.JobError("Cannot run stats on PII preview CSV: %s", e)

        pii_stats = str(qsv_pii_stats.stdout).strip()
        pii_stats_dict = [
            dict(id=ele.split(",")[0], type=conf.TYPE_MAPPING[ele.split(",")[1]])
            for idx, ele in enumerate(pii_stats.splitlines()[1:], 1)
        ]

        pii_resource = {
            "package_id": resource["package_id"],
            "name": resource["name"] + " - PII",
            "format": "CSV",
            "mimetype": "text/csv",
        }
        pii_response = dsu.send_resource_to_datastore(
            pii_resource,
            resource_id=None,
            headers=pii_stats_dict,
            records=None,
            aliases=pii_alias,
            calculate_record_count=False,
        )

        new_pii_resource_id = pii_response["result"]["resource_id"]

        # now COPY the PII preview to the datastore
        logger.info(
            'ADDING PII PREVIEW in "%s" with alias "%s"...',
            new_pii_resource_id,
            pii_alias,
        )
        col_names_list = [h["id"] for h in pii_stats_dict]
        column_names = sql.SQL(",").join(sql.Identifier(c) for c in col_names_list)

        copy_sql = sql.SQL(
            "COPY {} ({}) FROM STDIN "
            "WITH (FORMAT CSV, "
            "HEADER 1, ENCODING 'UTF8');"
        ).format(
            sql.Identifier(new_pii_resource_id),
            column_names,
        )

        with open(qsv_searchset_csv, "rb") as f:
            try:
                cur_pii.copy_expert(copy_sql, f)
            except psycopg2.Error as e:
                raise utils.JobError("Postgres COPY failed: %s", e)
            else:
                pii_copied_count = cur_pii.rowcount

        raw_connection_pii.commit()
        cur_pii.close()

        pii_resource["id"] = new_pii_resource_id
        pii_resource["pii_preview"] = True
        pii_resource["pii_of_resource"] = resource["id"]
        pii_resource["total_record_count"] = pii_rows_with_matches
        dsu.update_resource(pii_resource)

        pii_msg = "%d PII candidate/s in %d row/s are available at %s for review" % (
            pii_total_matches,
            pii_copied_count,
            resource["url"][: resource["url"].find("/resource/")]
            + "/resource/"
            + new_pii_resource_id,
        )
        if pii_found_abort:
            raise utils.JobError(pii_msg)
        else:
            logger.warning(pii_msg)
            logger.warning(
                "PII CANDIDATE/S FOUND but proceeding with job per Datapusher+ configuration."
            )
    elif not pii_found:
        logger.info("PII Scan complete. No PII candidate/s found.")

    return pii_found
