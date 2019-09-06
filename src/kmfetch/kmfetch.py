# !/usr/local/bin/python3.6
# MIT licensed
# Copyright (c) 2018 White Russsian
# Github: <https://github.com/Eve-PySpy/PySpy>**********************
"""
This script performs the following tasks:
    1.  Obtain new killmail id and has pairings available on zKillboard's
        API and stores them in a local MongoDB.

    2.a.Download the killmails from CCP's ESI API for each pairing
        obtained under step 1 and stores them in a separate collection of
        the same local MongoDB.

    2.b.Retry to download any killmails that failed to download in step 2.a.

    3.  Create summary killmails in preparation for use in step 4.
        steps and store them in the MongoDB.

    4.  Upload the victim and attacker summaries
        to the remote MySQL database via a http API.
"""
# **********************************************************************
import datetime
import logging
import json
import multiprocessing as mp
import sys
import time
import threading

from pymongo import MongoClient, InsertOne
import pymongo
import requests

import config
import db
# cSpell Checker - Correct Words****************************************
# // cSpell:words pymongo, russsian, strftime, zkill, zkillboard,
# // cSpell:words strptime, pyspy, bson, yyyymmdd, ccp's, cyno, cond,
# // cSpell:words vics, upsert, noval, objectid, SQL_CON, summarised,
# // cSpell:words sqlite, uncomment, getsizeof
# **********************************************************************
Logger = logging.getLogger(__name__)
# Example call: Logger.info("Something badhappened", exc_info=True) ****

MAX_RETRY = 5  # For zKillboard km hash downloads
MAX_MONGO_RETRY = 3  # Number of times to try to reconnect to Mongo DB
MAX_THREADS = 50  # For ESI KM downloads
ZKILL_OLDEST_DATE_INT = 20071205  # Date of the oldest killmail on zKillboard

SESSION_START_TIME = str(datetime.datetime.today()) # Used in db.esi_retry
MAX_UPLOAD_CHUNK = 5 * 1024 * 1024  # Used in upload_to_pyspy()
ZKILL_DAYS_REDOWNLOAD = -30  # Needs to be negative
SUMMARISED_KM_IDS = []  # Global list of all killmail ids processed in this run

# **********************************************************************
# 1. GET KM HASHES FROM ZKILLBOARD =====================================


def get_km_hashes(retry_dates=[]):
    """
    Downloads the daily killmail IDs including hashes from zKillboard's
    API and stores them in MongoDB collection for further processing.
    If `retry_dates` is not given, all dates between yesterday and the
    latest date already stored in the MongoDB are queried. Otherwise, only
    the dates provided in `retry_dates` are queried.

    :param `retry_dates`: List of Python date objects.

    :return: number of new killmail hashes downloaded as integer
    :return: any dates for which hashes could not be downloaded as list
        of integers.
    """
    # min_date = datetime.date(2007, 12, 5)  # Date of oldest KM on zKillboard

    num_km_hashes = 0
    failed_dates = []

    if not retry_dates:
        try:
            next_date = datetime.datetime.strptime(
                str(int(COL_ZKILL.find_one(sort=[("date", -1)])["date"])),
                '%Y%m%d'
                ).date() + datetime.timedelta(days=ZKILL_DAYS_REDOWNLOAD)
        except:
            next_date = datetime.datetime.strptime(
                str(ZKILL_OLDEST_DATE_INT),
                '%Y%m%d'
                ).date()

        # Return if there would not be any new hashes available yet
        if next_date > datetime.date.today():
            return num_km_hashes, failed_dates

        dates = daterange(
            next_date,
            datetime.date.today()
            )
    else:
        dates = retry_dates

    for query_date in dates:
        date_str = query_date.strftime("%Y%m%d")
        url = "https://zkillboard.com/api/history/" + date_str + "/"
        headers = {
            "Accept-Encoding": "gzip",
            "User-Agent": "PySpy, Author: White Russsian, https://github.com/WhiteRusssian/PySpy"
            }
        attempt = 0
        next = False
        r = None
        while next is False and attempt < MAX_RETRY:
            try:
                r = requests.get(url, headers=headers)
                if r.status_code == 200:
                    next = True
            except:
                attempt += 1
            time.sleep(pow(2, attempt))
        if r is not None and r.status_code == 200:
            store_km_hashes(date_str, r.json())
            num_km_hashes += len(r.json())
        else:
            Logger.warning("[get_km_hashes] Failed to get killmail hashes for: " + date_str)
            failed_dates.append(date_str)
            break
    return num_km_hashes, failed_dates


def daterange(start_date, end_date):
    """
    Creates a range iterable of dates between two dates. For consistency
    with the built-in range() function this iteration stops before reaching the
    end_date.

    :param: `start_date` The first date in the range.
    :param: `end_date` The end of the range, not included.
    """
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


def store_km_hashes(date_str, records):
    """
    Stores id and hash parings from zKillboard.

    :param: `date` Date as string.
    :param: `records` the killmail_ids and related hashes for that day.
    """
    inserts = []
    for id_str, hash in records.items():
        query = InsertOne(
            {
                "date": int(date_str),
                "kill_id": int(id_str),
                "hash": hash
                }
            )
        inserts.append(query)
    mongo_attempt = 0
    while mongo_attempt <= MAX_MONGO_RETRY:
        mongo_attempt += 1
        try:
            # By setting ordered=False, we ignore duplicates and still process
            # new unique values, causes a BulkWriteError, which we ignore
            Logger.info(
                "[store_km_hashes] Downloaded " + str(len(inserts)) +
                " killmails for date: " + date_str
                )
            COL_ZKILL.bulk_write(inserts, ordered=False)
            break
        except pymongo.errors.BulkWriteError:
            break
        except pymongo.errors.AutoReconnect:
            time.sleep(pow(5, mongo_attempt))
            Logger.warning(
                "[store_km_hashes] Mongo Disconnect. For date " +
                date_str + ", reconnect attempt: " + str(mongo_attempt)
                )


# **********************************************************************
# 2. DOWNLOAD KM DETAILS FROM ESI ======================================


def download_esi_kms(zkill_latest_date=0):
    """
    Downloads CCP ESI killmail data for kill_id and hash pairing
    in `zkill_kms` related to dates after `zkill_latest_date`. Will not
    download killmail data if that data is already available in `esi_kms`.

    :param: `zkill_latest_date` Date as integer in format YYYYMMDD.

    :return: Number of killmails that were processed which is not
    necessarily the number of killmails actually downloaded, in case
    there were any issues with the download.

    :return: Number and list of `killmail_ids` which have should have
    been downloaded, subject to any download failures recorded in the `retry`
    table.
    """

    Logger.info("[download_esi_kms] Checking for killmails to be downloaded from ESI.")

    cursor = list(
        COL_ZKILL.aggregate([
            {"$match":{"date": {"$gt": zkill_latest_date}}},
            {"$sort": {"date":1}},
            {"$group":{"_id": "$date"}},
            {"$project":{"date":"$date"}}
            ])
        )
    zkill_dates = list(map(lambda r: int(r['_id']), cursor))

    new_km_ids = []
    count = 0
    total = len(zkill_dates)
    for date in zkill_dates:
        count += 1
        missing_km_ids = []
        cursor = list(
            COL_ZKILL.find(
                {"date": date},
                {"_id": 0, "kill_id": 1}
                ).sort("kill_id")
            )

        zkill_kills = list(map(lambda r: r['kill_id'], cursor))

        cursor = list(
            COL_ESI.find(
                {"killmail_date": date},
                {"_id": 0, "killmail_id": 1}
                ).sort("killmail_id")
            )
        esi_kills = list(map(lambda r: r['killmail_id'], cursor))

        missing_km_ids = list(set(zkill_kills) - set(esi_kills))

        new_km_ids += missing_km_ids

        print(
            "Progress: " + "{:.3%}".format(count/total) +
            ". Total missing so far: " + str(len(new_km_ids)),
            end='\r'
            )

    Logger.info(
        "[download_esi_kms] Number of killmails to be downloaded from ESI: " +
        str(len(new_km_ids))
        )
    # Split workload across 2 processes (2 CPU cores on my server)
    if len(new_km_ids) > 0:
        num_ids = len(new_km_ids)
        split_num = int(num_ids / 2)
        proc_1_ids = new_km_ids[:split_num]
        proc_2_ids = new_km_ids[split_num:]

        proc_1 = mp.Process(
            target=esi_threads,
            args=(proc_1_ids,)
        )
        proc_1.start()
        Logger.info("[download_esi_kms] Process 1 started.")

        proc_2 = mp.Process(
            target=esi_threads,
            args=(proc_2_ids,)
        )
        proc_2.start()
        Logger.info("[download_esi_kms] Process 2 started.")

        proc_1.join()
        Logger.info("[download_esi_kms] Process 1 finished.")
        proc_2.join()
        Logger.info("[download_esi_kms] Process 2 finished.")

        record_new_km_ids(new_km_ids)

    return len(new_km_ids), new_km_ids


def esi_threads(kill_ids):
    """
    Starts a new thread for each download request from CCP's ESI.

    :param: `kill_ids` A list of integers of killmail_ids.
    """
    # Since `esi_threads` gets launched in new process (not thread), and
    # because MongoDB is not fork-safe, we need to create separate DB
    # connections for each process.
    client_fork = MongoClient(config.MONGO_SERVER_IP)
    db_fork = client_fork.pyspy
    col_zkill_fork = db_fork.zkill_kms
    col_esi_fork = db_fork.esi_kms
    col_esi_retry_fork = db_fork.retry

    for kill_id in kill_ids:
        hash = col_zkill_fork.find_one({"kill_id": kill_id})["hash"]
        while True:
            if threading.active_count() <= MAX_THREADS:
                t = threading.Thread(
                    target=get_kill_details,
                    daemon=False,
                    args=(kill_id, hash, col_esi_fork, col_esi_retry_fork)
                    )
                t.start()
                time.sleep(0.02)
                break
            else:
                time.sleep(0.1)


def get_kill_details(id, hash, col_esi_fork, col_esi_retry_fork):
    """
    Downloads a killmail from CCP's ESI and stores it in `DB.esi_kms`.
    Adds the field `killmail_date` as integer in format YYYYMMDD. Where
    the download failed, the id and hash pairing is written to `DB.retry`.

    :param: `id` Killmail_id as integer.
    :param: `hash` CCP killmail hash as string.
    """
    url = (
        "https://esi.evetech.net/latest/killmails/" +
        str(id) + "/" +
        hash + "/?datasource=tranquility"
        )
    headers = {
        "Accept-Encoding": "gzip",
        "User-Agent": "PySpy, Author: White Russsian, https://github.com/WhiteRusssian/PySpy"
        }
    attempt = 0
    while attempt <= MAX_RETRY:
        attempt += 1
        try:
            r = requests.get(url, headers=headers)
            if r.status_code == 200:  # If successful
                break
            if r.status_code == 422:  # If ID / hash data is wrong
                Logger.info("[get_kill_details] Killmail not available on ESI: " + str(id))
                break
        except requests.exceptions.ConnectionError:
            if attempt == MAX_RETRY:
                Logger.warning("[get_kill_details] ESI Connection Error: " + url)
        except Exception as e:
            if attempt == MAX_RETRY:
                Logger.warning("[get_kill_details] ESI error: " + str(e))
        # If there was a problem with the ESI connection, sleep and try again
        time.sleep(pow(2, attempt))

    mongo_attempt = 0
    if r is not None and r.status_code == 200:
        kill_dict = r.json()
        # Generate killmail_date in same format as in DB.zkill_kms as integer
        kill_time = kill_dict["killmail_time"]
        # "killmail_time" : "2018-07-12T00:00:39Z"
        kill_dict["killmail_date"] = int(
            kill_time[0:4] + kill_time[5:7] + kill_time[8:10]
            )
        while mongo_attempt <= MAX_MONGO_RETRY:
            mongo_attempt += 1
            try:
                col_esi_fork.insert_one(kill_dict)
                print("Downloaded killmail_id: " + str(id), end='\r')
                break
            except pymongo.errors.DuplicateKeyError:
                print("Killmail_id duplicate ignored:" + str(id))
            except pymongo.errors.AutoReconnect:
                time.sleep(pow(5, mongo_attempt))
                Logger.warning("[get_kill_details] Mongo Disconnect. Reconnect attempt: " + str(mongo_attempt))
            return
    # If ESI returned an error, other than "Invalid killmail_id and/or killmail_hash"
    # then add this id / hash pair to the retry table
    elif r is not None and r.status_code != 422:
        while mongo_attempt <= MAX_MONGO_RETRY:
            mongo_attempt += 1
            try:
                col_esi_retry_fork.insert_one({
                    "kill_id": id,
                    "hash": hash,
                    "error": r.status_code,
                    "session": SESSION_START_TIME
                    })
                break
            except pymongo.errors.AutoReconnect:
                time.sleep(pow(5, mongo_attempt))
                Logger.warning("[get_kill_details] Mongo Disconnect. Reconnect attempt: " + str(mongo_attempt))
            return
    # If ESI returned error "Invalid killmail_id and/or killmail_hash", do not
    # add id / hash pair to retry database
    elif r is not None and r.status_code == 422:
        return


def retry_failed_esi_downloads():
    """
    Gets the kill_ids that failed to download during the last run and
    tries to download them again.

    :return: List of kill_ids that were attempted to be re-downloaded as.
    """
    try:
        last_session = list(
            COL_ESI_RETRY.find({}).sort("session", -1).limit(1)
            )[0]["session"]
    except IndexError:
        return []

    retry_ids = COL_ESI_RETRY.distinct(
        "kill_id", {"session": last_session}
        )

    Logger.info(
        "[retry_failed_esi_downloads] Number of killmails to be re-download: " +
        str(len(retry_ids))
        )

    global SESSION_START_TIME
    SESSION_START_TIME = str(datetime.datetime.today())

    esi_threads(retry_ids)

    return retry_ids


def record_new_km_ids(new_km_ids):
    """
    Record ids of killmails in COL_NEW that have been added to COL_ESI
    but not yet summarised in Sqlite table.

    :param: `new_km_ids` List of killmail ids as integers.
    """
    inserts = []
    for id in new_km_ids:
        inserts.append(
            InsertOne({"killmail_id": int(id)})
            )
    mongo_attempt = 0
    while mongo_attempt <= MAX_MONGO_RETRY:
        mongo_attempt += 1
        try:
            # By setting ordered=False, we ignore duplicates and still process
            # new unique values
            COL_NEW.bulk_write(inserts, ordered=False)
            Logger.info(
                "[record_new_km_ids] Recorded " + str(len(inserts)) +
                " new killmails waiting for summary processing."
                )
            break
        except pymongo.errors.BulkWriteError:
            pass
        except pymongo.errors.AutoReconnect:
            time.sleep(pow(5, mongo_attempt))
            Logger.warning(
                "[record_new_km_ids] Mongo Disconnected. Failed to record " +
                "the following new killmail_ids: " + str(new_km_ids)
                )


def processed_failed_ids():
    """
    Tries to redownload any killmails recorded in COL_RETRY, if any, where
    zkill had killmail data available but details could not be obtained
    from CCP previously.
    """
    attempt = 0
    while attempt <= MAX_RETRY:
        attempt += 1
        Logger.info(
            "[processed_failed_ids] Re-downloading missing ESI killmails. Attempt: " +
            str(attempt)
            )
        retry_ids = retry_failed_esi_downloads()
        Logger.info(
            "[processed_failed_ids] Number of ids to be re-downloaded: " + str(len(retry_ids))
            )
        if len(retry_ids) > 0:
            create_km_summaries(retry_ids)
            # Get any Killmail_ids that the retry run was still unable to download
            missing_kills = COL_ESI_RETRY.distinct(
                "kill_id", {"session": SESSION_START_TIME}
                )
            if len(missing_kills) == 0:
                Logger.info("[processed_failed_ids] Successfully re-downloaded all missing killmails.")
                COL_ESI_RETRY.delete_many({})
                break
            elif attempt == MAX_RETRY:
                Logger.warning(
                    "[processed_failed_ids] Unable to download the following killmail_ids: " +
                    str(missing_kills)
                )
                COL_ESI_RETRY.delete_many({})
                # Important step to delete any id / hash pairs from COL_NEW
                # that could not be found on ESI.
                COL_NEW.delete_many({"killmail_id":{'$in': missing_kills}})
                break
        else:
            break

# **********************************************************************
# 3. PREPARE KM_SUMMARY COLLECTION =====================================


def create_km_summaries(retry_ids=None):
    """
    Populates the sqlite `km_summary` table with summary killmail
    information for each killmail stored in `DB.esi_kms`.

    :return: List of killmail_ids summarised in km_summary.
    """

    process_ids = []

    # First check if certain killmails have to be redownloaded
    if isinstance(retry_ids, (list, tuple)):
        process_ids = COL_ESI.distinct(
            "killmail_id",
            {"killmail_id": {"$in": retry_ids}}
            )
        Logger.info(
            "[create_km_summaries] Found retry killmails: " +
            str(len(process_ids))
        )

    # Then check if only a small number of new killmails has to be processed
    if not process_ids:
        process_ids = COL_NEW.distinct("killmail_id")
        Logger.info(
            "[create_km_summaries] Found new killmails: " +
            str(len(process_ids))
        )

    # If none of the above cases apply, check for any killmails that are available
    # in COL_ESI but not in km_summary and process those
    if not process_ids:
        Logger.info("[create_km_summaries] Going to check all dates.")

        min_esi_year = int(COL_ESI.find_one(sort=[("killmail_date", 1)])['killmail_date']/10000)
        max_esi_year = int(COL_ESI.find_one(sort=[("killmail_date", -1)])['killmail_date']/10000)
        esi_years = list(range(min_esi_year, max_esi_year + 1))

        # esi_years = [2018]  # Uncomment for testing purposes
        for year in esi_years:
            for month in range(1, 13):
                start_date = year * 10000 + month * 100
                end_date = year * 10000 + (month + 1) * 100
                esi_ids = COL_ESI.distinct(
                    "killmail_id",
                    {
                        "killmail_date": {"$gt": start_date, "$lt": end_date},
                        "victim.character_id": { "$exists": True }
                    }
                    )

                cursor = SQL_CON.execute(
                    '''SELECT DISTINCT killmail_id FROM km_summary
                    WHERE killmail_date > ? AND killmail_date < ?
                    ''', (start_date, end_date)
                    )

                sum_ids = list(map(lambda r: r[0], cursor))

                this_month_ids = list(set(esi_ids) - set(sum_ids))
                process_ids += this_month_ids
                print(
                    "Current month: " + str(start_date) +
                    " / Total missing KMs: " +
                    "{:,}".format(len(process_ids)), end='\r'
                    )

        Logger.info(
            "[create_km_summaries] Full scan of all killmails. To be processed: " +
            str(len(process_ids))
            )

    kms_processed = 0
    structure_kms = 0

    for id in process_ids:
        kms_processed += 1

        km = COL_ESI.find_one({"killmail_id": id})
        # It is possible that the killmail failed to download previously
        if km is None:
            Logger.warning(
                "[create_km_summaries] Could not find killmail_id: " + str(id) +
                " Attempting to download again..."
                )
            hash = COL_ZKILL.find_one({"kill_id":id})["hash"]
            get_kill_details(id, hash, COL_ESI, COL_ESI_RETRY)
            km = COL_ESI.find_one({"killmail_id": id})
            # If killmail is still not downloaded, throw error and go to next id
            if km is None:
                Logger.error(
                    "[create_km_summaries] Unable to download kill: " + str(id)
                    )
                continue
            else:
                Logger.info(
                    "[create_km_summaries] Successfully downloaded killmail " +
                    "with id: " + str(id) + ". Processing summary now."
                    )

        if not "character_id" in km["victim"]:  # not interested in structure kills
            structure_kms += 1
            COL_NEW.delete_many({"killmail_id": id})
            print("KM summary progress: " + "{:.3%}".format(kms_processed / len(process_ids)), end='\r')
            continue
        killmail_id = km["killmail_id"]
        killmail_date = km["killmail_date"]
        solar_system_id = km["solar_system_id"]
        if 32000200 >= solar_system_id >= 32000001:
            abyssal = True
        else:
            abyssal = False
        victim_id = km["victim"]["character_id"]
        ship_id = km["victim"]["ship_type_id"]
        covert_cyno = False
        normal_cyno = False
        if "items" in km["victim"]:
            for item in km["victim"]["items"]:
                # Check if victim ship had covert cyno fitted (to high slot)
                if item["item_type_id"] == 28646 and item["flag"] >= 27 and item["flag"] <=34:
                    covert_cyno = True
                # Check if victim ship had normal cyno fitted (to high slot)
                if item["item_type_id"] == 21096 and item["flag"] >= 27 and item["flag"] <=34:
                    normal_cyno = True
        attacker_count = 0
        if "attackers" in km:
            attacker_ids = []
            for attacker in km["attackers"]:
                if "character_id" in attacker:  # would not be the case for NPC KMs
                    attacker_count += 1
                    attacker_ids.append(attacker["character_id"])
            attackers_data = []
            for attacker_id in attacker_ids:
                attackers_data.append(
                    (killmail_id, killmail_date, attacker_id, attacker_count)
                    )
            SQL_CON.executemany(
                '''REPLACE INTO attackers
                (killmail_id, killmail_date, attacker_id, attacker_count)
                VALUES (?, ?, ?, ?)''',
                attackers_data
                )

        summary = (
            killmail_id,
            killmail_date,
            solar_system_id,
            abyssal,
            attacker_count,
            victim_id,
            ship_id,
            covert_cyno,
            normal_cyno
            )
        SQL_CON.execute(
            '''REPLACE INTO km_summary (killmail_id, killmail_date,
            solar_system_id, abyssal, attacker_count, victim_id,
            ship_id, covert_cyno, normal_cyno)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', summary
            )
        SQL_CON.commit()

        SUMMARISED_KM_IDS.append(id)

        COL_NEW.delete_many({"killmail_id": id})

        print("KM summary progress: " + "{:.3%}".format(kms_processed / len(process_ids)), end='\r')

    Logger.info("[create_km_summaries] Number of killmails processed: " + str(kms_processed))
    Logger.info("[create_km_summaries] Number of structure killmails ignored: " + str(structure_kms))


# **********************************************************************
# 4. UPDATE ONLINE DATABASE ============================================

def update_public_database(new_km_ids=[]):
    """
    Uploads new victim and attacker data to the remote database. If
    `new_km_ids` is not provided, all victim and attacker records will
    be uploaded.

    :param :`new_km_ids` List of recently added killmail ids as integers.
    """

    cur = SQL_CON.cursor()

    if len(new_km_ids) > 0:
        Logger.info(
            "[update_public_database] Getting list of touched victim " +
            "and attacker ids."
            )
        touched_victims = get_touched_victims(new_km_ids)
        touched_attackers = get_touched_attackers(new_km_ids)
        Logger.info(
            "[update_public_database] Identified " + str(len(touched_victims)) +
            " victims and " + str(len(touched_attackers)) +
            " attackers for which the remote database needs to be updated."
             )

        vic_data = cur.execute(
            '''SELECT * FROM vic_sum WHERE victim_id IN
            (%s)''' % (', '.join(['?'] * len(touched_victims))),
            touched_victims
            ).fetchall()
        vic_headers = cur.description

        atk_data = cur.execute(
            '''SELECT * FROM atk_sum WHERE attacker_id IN
            (%s)''' % (', '.join(['?'] * len(touched_attackers))),
            touched_attackers
            ).fetchall()
        atk_headers = cur.description

    else:
        Logger.info(
            '''[update_public_database] Gathering summary stats for all victims.'''
            )
        vic_data = cur.execute(
            '''SELECT * FROM vic_sum'''
            ).fetchall()
        vic_headers = cur.description
        Logger.info(
            '''[update_public_database] Victim stats finished.'''
            )

        Logger.info(
            '''[update_public_database] Gathering summary stats for all attackers.'''
            )
        atk_data = cur.execute(
            '''SELECT * FROM atk_sum'''
            ).fetchall()
        atk_headers = cur.description
        Logger.info(
            '''[update_public_database] Attacker stats finished.'''
            )

    Logger.info("[update_public_database] Begin victim data upload.")
    prepare_upload_data(vic_data, vic_headers)
    Logger.info("[update_public_database] Victim data uploaded.")

    Logger.info("[update_public_database] Begin attacker data upload.")
    prepare_upload_data(atk_data, atk_headers)
    Logger.info("[update_public_database] Attacker data uploaded.")

    cur.close()


# **********************************************************************
# HELPER FUNCTIONS =====================================================


def get_touched_victims(new_km_ids):
    """
    Returns a list of character_ids that were involved in new killmails.

    :return: List of character ids.
    """

    touched_victims = list(
        SQL_CON.execute(
            '''SELECT DISTINCT(victim_id) FROM km_summary
            WHERE killmail_id IN (%s)''' % (', '.join(['?'] * len(new_km_ids))),
            new_km_ids
            ).fetchall()
        )

    return tuple(r[0] for r in touched_victims)


def get_touched_attackers(new_km_ids):
    """
    Returns a list of character_ids that were involved in new killmails.

    :return: List of character ids.
    """

    touched_attackers = list(
        SQL_CON.execute(
            '''SELECT DISTINCT(attacker_id) FROM attackers
            WHERE killmail_id IN (%s)''' % (', '.join(['?'] * len(new_km_ids))),
            new_km_ids
            ).fetchall()
        )

    return tuple(r[0] for r in touched_attackers)


def prepare_upload_data(data, headers):
    """
    Takes sqlite query results and related column headers and splits the
    data into smaller chunks of json strings for upload to the remote db.

    :param: `data` List or tuple of sqlite query results.
    :param: `headers` List or tuple of sqlite column headers.
    """

    # Calculate the size of the JSON string to be uploaded
    upload_size = sys.getsizeof(
        json.dumps(
            [dict(zip([key[0] for key in headers], row)) for row in data]
            )
        )

    chunk_count = int(upload_size / MAX_UPLOAD_CHUNK) + 1
    num_rows = len(data)
    chunk_rows = int(num_rows / chunk_count)

    start = 0
    while start < num_rows:
        end = start + chunk_rows
        upload_set = data[start:end]
        json_string = json.dumps([
            dict(zip(
                [key[0] for key in headers], row)) for row in upload_set
            ])
        upload_status = upload_to_pyspy(json_string)
        if not upload_status:
            Logger.error(
                "[prepare_upload_data] Failed to complete PySpy uploads."
                )
            break
        Logger.info(
            "[prepare_upload_data] Progress: " +
            "{:.1%}".format(min(end, num_rows) / num_rows)
            )
        start = end

    Logger.info(
        "[prepare_upload_data] Successfully uploaded: " + "{:,}".format(num_rows) +
        " records with total size of: " + "{:0,.1f}".format(upload_size/1024/1024) +
        " MB."
        )


def upload_to_pyspy(json_data):
    """
    Takes JSON data and uploads it to remote database. JSON structure must
    match structure of remote flask application and underlying database.

    :param: `json_data` JSON object as string.
    :return: `True` if upload successful, else `False`.
    """

    url = "http://{}:{}@{}".format(config.PYSPY_USER, config.PYSPY_PWD, config.UPLOAD_URL) + "/upload/v1/"

    attempt = 0
    while attempt <= MAX_RETRY:
        attempt += 1
        try:
            r = requests.post(url, json=json_data)
            Logger.info(
                "[upload_to_pyspy] Response code: " +
                str(r.status_code) + ", " + r.text
                )
            return True

        except requests.ConnectionError:
            Logger.warning(
                "[upload_to_pyspy] Could not connect to remote database. Attempt: " +
                str(attempt)
                )
            time.sleep(pow(2, attempt))
        except requests.RequestException as e:
            Logger.error(
                "[upload_to_pyspy] Could not connect to remote database. Attempt: " +
                str(attempt) + ". Exception message: " + str(e)
                )
    return False


def is_now_in_time_period(start_time, end_time, now_time):
    if start_time < end_time:
        return end_time >= now_time >= start_time
    else:  # Over midnight
        return now_time >= start_time or now_time <= end_time

# **********************************************************************
# SCRIPT CONTROL =======================================================


if __name__ == "__main__":

    # Setup database connection and relevant collections
    CLIENT = MongoClient(config.MONGO_SERVER_IP)
    DB = CLIENT.pyspy
    COL_ZKILL = DB.zkill_kms
    COL_ESI = DB.esi_kms
    COL_ESI_RETRY = DB.retry
    COL_NEW = DB.new_km_ids
    SQL_CON = db.connect_db()

    while True:

        if is_now_in_time_period(datetime.time(config.START_TIME), datetime.time(config.END_TIME), datetime.datetime.now().time()):

            # 0. Determine start date for step 2 (download from CCP)
            # Check

            next_date = int((
                datetime.datetime.strptime(
                    str(COL_ZKILL.find_one(sort=[("date", -1)])["date"]),
                    '%Y%m%d'
                    ).date() +
                datetime.timedelta(days=ZKILL_DAYS_REDOWNLOAD)
                ).strftime("%Y%m%d"))

            zkill_latest_date = max(
                next_date,
                ZKILL_OLDEST_DATE_INT
                )

            Logger.info("[main_0] Downloading killmails from " + str(zkill_latest_date) + " and onward.")

            # 1. Download killmail_ids and hashes from zKillboard
            Logger.info("[main_1] Checking zkill for new killmail hashes.")
            num_km_hashes, failed_dates = get_km_hashes()
            Logger.info("[main_1] Number of new killmails on zKillboard: " + str(num_km_hashes))
            if failed_dates:
                Logger.warning("[main_1] Failed to obtain killmails for the following dates: " + str(failed_dates))

            # 2.a. Download killmail details from CCP
            Logger.info("[main_2a] Downloading killmail details from CCP.")
            num_kms_added, new_km_ids = download_esi_kms(zkill_latest_date)
            # num_kms_added, new_km_ids = download_esi_kms()
            Logger.info("[main_2a] Number of killmails downloaded: " + str(num_kms_added))

            # 2.b. Retry failed ESI downloads
            Logger.info("[main_2b] Checking for killmails to be redownloaded and processed.")
            processed_failed_ids()
            Logger.info("[main_2b] Finished redownloading all missing killmails.")

            if num_kms_added == 0:
                Logger.info("[main_2a] No new killmails downloaded. Exiting.")
                exit(0)

            # 3. Create killmail summary collection
            create_km_summaries()
            Logger.info(
                "[main_3] Number of killmail summaries created: " +
                str(len(SUMMARISED_KM_IDS))
                )

            # 4. Update PySpy online database with new victim and attacker data
            Logger.info("[main_4] Start uploading victim and attacker data to remote DB.")
            update_public_database(SUMMARISED_KM_IDS)
            #update_public_database()
            Logger.info("[main_4] Finished uploading victim and attacker data to remote DB.")

            SQL_CON.execute("PRAGMA optimize")
            SQL_CON.close()

            Logger.info("EXITING SCRIPT ==============================================")
        else:
            Logger.info("Time has not yet come")
        Logger.info("Sleeping for an hour")
        time.sleep(config.SLEEP_TIME)
        Logger.info("Done Sleeping")
