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


def _is_searchset_no_match(result) -> bool:
    """True if a ``qsv searchset`` run failed only because nothing matched.

    qsv exits 1 for "no match" and prints nothing, or, when asked for
    structured errors (``QSV_ERROR_FORMAT=json``), a single JSON error line
    of kind ``no_match``. Any other failure prints an error message.
    """
    if result.returncode != 1:
        return False
    stderr = (result.stderr or "").strip()
    if not stderr:
        return True
    try:
        err = json.loads(stderr.splitlines()[-1]).get("error", {})
    except (ValueError, AttributeError):
        return False
    return isinstance(err, dict) and err.get("kind") == "no_match"


def screen_for_pii(
    tmp: str,
    resource: dict,
    qsv: QSVCommand,
    temp_dir: str,
    logger: logging.Logger,
) -> tuple[bool, int]:
    """
    Screen a file for Personally Identifiable Information (PII) using qsv's searchset command.

    Args:
        tmp: Path to the file to screen
        resource: Resource dictionary containing metadata
        qsv: QSVCommand instance
        temp_dir: Temporary directory path
        logger: Logger instance

    Returns:
        tuple[bool, int]: ``(pii_found, pii_candidate_count)``. The
        non-quick path reports an exact match count; the quick path can
        only detect presence and reports a degenerate ``1``.
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
        if not pii_regex_resource_exist:
            raise utils.JobError(
                f"PII regex resource {conf.PII_REGEX_RESOURCE_ID!r} not found in the "
                "DataStore. Check ckanext.datapusher_plus.pii_regex_resource_id_or_alias, "
                "or unset it to use the default PII regexes."
            )
        if pii_regex_resource_exist:
            pii_resource = dsu.get_resource(conf.PII_REGEX_RESOURCE_ID)
            pii_regex_url = pii_resource["url"]

            try:
                r = requests.get(pii_regex_url)
                r.raise_for_status()
                pii_regex_file = pii_regex_url.split("/")[-1]
                p = Path(__file__).with_name("user-pii-regexes.txt")
                with p.open("wb") as f:
                    f.write(r.content)
            except requests.RequestException as e:
                raise utils.JobError(f"Failed to fetch PII regex resource: {e}")
    else:
        pii_regex_file = "default-pii-regexes.txt"
        p = Path(__file__).with_name(pii_regex_file)

    pii_found = False
    # Count of PII candidate matches, surfaced to the caller so the
    # v3.0 PII-review suspend gate has a real number to threshold on.
    pii_candidate_count = 0
    # Both modes search the same file. It must be an absolute path: the
    # worker's current directory is not the package directory.
    pii_regex_fname = p.absolute()

    if conf.PII_QUICK_SCREEN:
        logger.info("Quickly scanning for PII using %s...", pii_regex_file)
        try:
            qsv_searchset = qsv.searchset(
                pii_regex_fname,
                tmp,
                ignore_case=True,
                quick=True,
                check=False,
            )
        except utils.JobError as e:
            raise utils.JobError(f"Cannot quickly search CSV for PII: {e}")
        # Exit 0: a match, with its row number on stderr. Exit 1 with no
        # error message: no match, so no PII. (``--not-one`` can't be used:
        # with it, qsv prints the row count to stderr even when nothing
        # matched, which reads as a match.)
        pii_candidate_row = ""
        if qsv_searchset.returncode == 0:
            pii_candidate_row = str(qsv_searchset.stderr)
            pii_found = True
            # Quick screen only detects presence, not a count.
            pii_candidate_count = 1
        elif not _is_searchset_no_match(qsv_searchset):
            raise utils.JobError(
                f"Cannot quickly search CSV for PII: {qsv_searchset.stderr}"
            )

    else:
        logger.info("Scanning for PII using %s...", pii_regex_file)
        qsv_searchset_csv = os.path.join(temp_dir, "qsv_searchset.csv")
        try:
            qsv_searchset = qsv.searchset(
                pii_regex_fname,
                tmp,
                ignore_case=True,
                flag="PII_info",
                flag_matches_only=True,
                json_output=True,
                output_file=qsv_searchset_csv,
            )
        except utils.JobError as e:
            raise utils.JobError(f"Cannot search CSV for PII: {e}")
        pii_json = json.loads(str(qsv_searchset.stderr))
        pii_total_matches = int(pii_json["total_matches"])
        pii_rows_with_matches = int(pii_json["rows_with_matches"])
        if pii_total_matches > 0:
            pii_found = True
            pii_candidate_count = pii_total_matches

    if pii_found and pii_found_abort and not conf.PII_SHOW_CANDIDATES:
        logger.error("PII Candidate/s Found!")
        if conf.PII_QUICK_SCREEN:
            raise utils.JobError(
                f"PII CANDIDATE FOUND on row {pii_candidate_row.rstrip()}! Job aborted."
            )
        else:
            raise utils.JobError(
                f"PII CANDIDATE/S FOUND! Job aborted. Found {pii_total_matches} PII "
                f"candidate/s in {pii_rows_with_matches} row/s."
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
            with psycopg2.connect(conf.DATASTORE_WRITE_URL) as raw_connection_pii:
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
                    raise utils.JobError(f"Cannot run stats on PII preview CSV: {e}")

                pii_stats = str(qsv_pii_stats.stdout).strip()
                pii_stats_dict = [
                    dict(
                        id=ele.split(",")[0], type=conf.TYPE_MAPPING[ele.split(",")[1]]
                    )
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
                column_names = sql.SQL(",").join(
                    sql.Identifier(c) for c in col_names_list
                )

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
                        raise utils.JobError(f"Postgres COPY failed: {e}")
                    else:
                        pii_copied_count = cur_pii.rowcount

                raw_connection_pii.commit()
        except psycopg2.Error as e:
            raise utils.JobError(f"Could not connect to the Datastore: {e}")

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

    return pii_found, pii_candidate_count
