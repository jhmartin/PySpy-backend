#!/usr/bin/env python3
import decimal
import sqlite3
import json
import logging.config
import logging
from io import BytesIO
import tarfile
import time
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
import requests
import boto3
# import config
import db

Logger = logging.getLogger(__name__)

# Example call: Logger.info("Something badhappened", exc_info=True) ****


def generate_date_range(start_date: str, end_date: str = (
        datetime.now() - timedelta(days=1)).strftime('%Y%m%d')) -> list:
    """
    Generates a list of dates between a specified start date and an optional end date in YYYYMMDD format.
    If end_date is not provided, it defaults to yesterday's date.

    Args:
        start_date (str): The start date in YYYYMMDD format.
        end_date (str, optional): The end date in YYYYMMDD format. Defaults to yesterday.

    Returns:
        list: A list of dates between the start and end dates, inclusive.
    """
    try:
        start = datetime.strptime(start_date, '%Y%m%d')
        end = datetime.strptime(end_date, '%Y%m%d')
        step = timedelta(days=1)

        date_list = []
        while start <= end:
            date_list.append(start.strftime('%Y%m%d'))
            start += step

        return date_list
    except ValueError as e:
        Logger.exception("Invalid date format", exc_info=True)
        raise e


def get_latest_killmail_date(connection: sqlite3.Connection) -> str:
    """
    Queries the km_summary table for the latest killmail_date.

    Args:
        connection (sqlite3.Connection): The SQLite connection object.

    Returns:
        str: The latest killmail_date in the table, or None if the table is empty.
    """
    query = """
        SELECT MAX(killmail_date) AS latest_killmail_date
        FROM km_summary
    """

    try:
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        return result['latest_killmail_date'] if result and result['latest_killmail_date'] is not None else None
    except sqlite3.Error as e:
        Logger.exception("sqlite errir occured", exc_info=True)
        raise e


def fetch_and_unpack_killmail(date: str):
    """
    Downloads and unpacks a tar.bz2 file containing killmail data for a given date.

    Args:
        date (str): The date in YYYYMMDD format for which to fetch killmail data.

    Returns:
        None: Prints the contents of the files within the tar archive.
    """
    # Construct the URL
    date_parsed = datetime.strptime(date, '%Y%m%d')
    year = date_parsed.strftime('%Y')
    month = date_parsed.strftime('%m')
    day = date_parsed.strftime('%d')
    url = f"https://data.everef.net/killmails/{
        year}/killmails-{year}-{month}-{day}.tar.bz2"

    # Define custom headers
    headers = {'User-Agent': 'PySpy-backend jhmartin@toger.us'}

    # Download the file with timeout and headers
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()
    if response.status_code == 200:
        # Unpack the tar.bz2 file in memory
        file_like_object = BytesIO(response.content)
        with tarfile.open(mode="r:bz2", fileobj=file_like_object) as tar:
            results = []
        # List contents or process files within the tar file
            for member in tar.getmembers():
                f = tar.extractfile(member)
                if f:
                    # Processing file contents, here just printing the file
                    # name
                    results.append(json.load(f))
                    # Example to read the file content: content = f.read()
            return results
    else:
        Logger.error(
            "Failed to download file from %s. Status code: %s",
            url,
            response.stastus_code)
        raise Exception("Unable to download file {response.status_code}")


def is_abyssal_system(system_id: int) -> bool:
    """ Take a system id and check if it is in the abyssal range"""
    if 32000200 >= system_id >= 32000001:
        return True
    return False


def has_normal_cyno(victim_dict: dict) -> bool:
    """ Check items on a victim if any match the normal cyno type"""
    if "items" in victim_dict:
        for item in victim_dict["items"]:
            if item["item_type_id"] == 21096 and item["flag"] >= 27 and item["flag"] <= 34:
                return True
    return False


def has_covert_cyno(victim_dict: dict) -> bool:
    """ Check items on a victim if any match the covert cyno type"""
    if "items" in victim_dict:
        for item in victim_dict["items"]:
            if item["item_type_id"] == 28646 and item["flag"] >= 27 and item["flag"] <= 34:
                return True
    return False


def get_killmail_date(killmail_time: str) -> int:
    """ Take a EVE killmail datetime and format a killmail date in YYYYMMDD"""
    return int(killmail_time[0:4] + killmail_time[5:7] + killmail_time[8:10])


def get_attacker_info(killmail: dict) -> (int, set[int]):
    """ Given a killmail, figure out how many attackers there were and their ids"""
    attacker_count = 0
    attacker_ids = set()
    if "attackers" in killmail:
        for attacker in killmail["attackers"]:
            if "character_id" in attacker:  # would not be the case for NPC KMs
                attacker_count += 1
                attacker_ids.add(attacker["character_id"])
    return (attacker_count, attacker_ids)


def create_killmail_summaries(killmails: list[dict]):
    """ Take a list of killmails and construct the KM summary info"""
    Logger.info("Killmail total is %s", len(killmails))
    summaries = []
    result_attackers = set()
    result_victims = set()
    for killmail in killmails:
        if 'victim' not in killmail or 'character_id' not in killmail['victim']:
            continue

        # "killmail_time" : "2018-07-12T00:00:39Z"
        killmail_id = killmail['killmail_id']
        killmail_date = get_killmail_date(killmail['killmail_time'])
        solar_system_id = killmail['solar_system_id']
        abyssal = is_abyssal_system(solar_system_id)
        (attacker_count, attacker_ids) = get_attacker_info(killmail)
        victim_id = killmail["victim"]["character_id"]
        ship_id = killmail["victim"]["ship_type_id"]
        covert_cyno = has_covert_cyno(killmail["victim"])
        normal_cyno = has_normal_cyno(killmail["victim"])

        result_attackers.update(attacker_ids)
        result_victims.add(victim_id)

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
        summaries.append(summary)
    Logger.info("Candidate killmails are %s", len(summaries))
    Logger.info("Distinct attackers is %s", len(result_attackers))
    return (summaries, result_attackers, result_victims)


def dict_factory(cursor, row):
    """ Used by the sqlite engine to convert a row to dict"""
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}


def clean_dict(in_dict: dict) -> dict:
    """ Remove all dict keys that have null/none values """
    return {k: v for k, v in in_dict.items() if v is not None}


def insert_item(item, dest_table):
    """ Upload a intel item to DynamoDB. Have to handle throughput exceptions with increasing delay retries.
    There is a batch upload command, but we have to handle partial success. Avoiding that is simpler for daily uploads"""
    item = json.loads(json.dumps(item), parse_float=decimal.Decimal)
    for attempt in range(1, 6):  # Retry up to 5 times
        try:
            dest_table.put_item(Item=item)
            return
        except ClientError as err:
            if err.response['Error']['Code'] not in [
                    "ProvisionedThroughputExceededException"]:
                raise err
            Logger.warning('WHOA, too fast, slow it down retries=%s', attempt)
            time.sleep(2 ** attempt)


def persist_killmail_summaries(km_summaries, sqlcon):
    """ Write the killmail summaries to sqlite"""
    Logger.info("Persisting killmails into sqlite")
    # Write to sql in a tight batch to reduce round-trops
    sqlcon.executemany(
        '''REPLACE INTO km_summary (killmail_id, killmail_date,
                    solar_system_id, abyssal, attacker_count, victim_id,
                    ship_id, covert_cyno, normal_cyno)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', km_summaries
    )
    sqlcon.commit()
    Logger.info("Finished persisting killmails into sqlite")


def fetch_victim_intel_js(victims, cursor) -> list:
    """ Given a list of victims, fetch their data out of the sqlite view"""
    Logger.info("Identified %s victims to update", len(victims))
    vic_data = cursor.execute(
        '''SELECT * FROM vic_sum WHERE character_id IN
            (%s)''' % (', '.join(['?'] * len(victims))),
        list(victims)
    ).fetchall()
    return vic_data


def fetch_attacker_intel_js(attackers, cursor) -> list:
    """ Given a list of attackers, fetch their data out of the sqlite view"""
    Logger.info("Identified %s attackers to update", len(attackers))
    atk_data = cursor.execute(
        '''SELECT * FROM atk_sum WHERE character_id IN
            (%s)''' % (', '.join(['?'] * len(attackers))),
        list(attackers)
    ).fetchall()
    return atk_data


session = boto3.Session(region_name='us-west-2')
dynamodb = session.resource('dynamodb')
table = dynamodb.Table('pyspy-intel')

if __name__ == "__main__":
    sql = db.connect_db()
    sql.row_factory = dict_factory
    cur = sql.cursor()
    latest_date = get_latest_killmail_date(sql)
    date_range = generate_date_range(str(latest_date))
    date_range = generate_date_range(str(20241217))

    for km_date in date_range:
        Logger.info("Processing data for %s", km_date)
        day_killmails = fetch_and_unpack_killmail(str(km_date))
        (killmail_summaries, touched_attackers,
         touched_victims) = create_killmail_summaries(day_killmails)
        persist_killmail_summaries(killmail_summaries, sql)

        # boto3.set_stream_logger('', logging.DEBUG)

        victim_data = fetch_victim_intel_js(touched_victims, cur)
        attacker_data = fetch_attacker_intel_js(touched_attackers, cur)

        all_data = victim_data + attacker_data
        total_len = len(all_data)
        PROGRESS = 0
        for update in all_data:
            Logger.info("Uploading %s/%s for %s", PROGRESS, total_len, km_date)
            PROGRESS += 1
            insert_item(update, table)

    sql.close()
