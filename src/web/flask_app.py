# !/usr/local/bin/python3.6
# MIT licensed
# Copyright (c) 2018 White Russsian
"""
Note: this script can be run locally for testing purposes, in which case
it will not import the MySQLdb module or try to connect or write to the
Pyspy database.
"""
# **********************************************************************
import time

from flask import Flask
from flask import request
from werkzeug.security import generate_password_hash, check_password_hash
from flask_httpauth import HTTPBasicAuth
import json
import os

# DO NOT IMPORT MYSQLDB IF RUN IN LOCAL TESTING MODE
if __name__ != '__main__':
    import MySQLdb

# cSpell Checker - Correct Words****************************************
# // cSpell:words russsian, pyspy, ipaddr, blops
# **********************************************************************

app = Flask(__name__)
auth = HTTPBasicAuth()

# Users which can be used to access "login_reuqire" routes
users = {
    "{}".format(os.environ.get("PYSPY_USER")): generate_password_hash("{}".format(os.environ.get("PYSPY_PWD")))
}

@auth.verify_password
def verify_password(username, password):
    """
    Verifies the password send by the user.
    """
    if username in users:
        return check_password_hash(users.get(username), password)
    return False


def connect_db():
    """
    Set up connection to Pyspy database.
    """
    db_server = "{}".format(os.environ.get("DB_SERVER"))
    db_user = "{}".format(os.environ.get("DB_USER"))
    db_pwd = "{}".format(os.environ.get("DB_PWD"))
    db_name = "{}".format(os.environ.get("DB_NAME"))
    conn = MySQLdb.connect(host=db_server, user=db_user, passwd=db_pwd, db=db_name)
    cur = conn.cursor()
    return conn, cur


@app.route("/add_record/")
def add_usage_record():
    '''
    Records usage statistics. Gets called everytime a Pyspy client
    performs a character scan. To somewhat prevent unintended access,
    the user agent has to match that of Pyspy. Admittedly, not a strong
    protection.
    '''
    # Check user agent. Not very strong protection but better than nothing.
    pyspy_useragent = "PySpy, Author: White Russsian, https://github.com/WhiteRusssian/PySpy"
    if request.user_agent.string != pyspy_useragent:
        return "Bad Request, not authorised."

    args = request.args
    ipaddr = request.headers.getlist("X-Forwarded-For")[0]
    record = [
        args.get("uuid"),
        ipaddr,
        args.get("version"),
        args.get("platform"),
        args.get("chars"),
        args.get("duration"),
        int(args.get("sh_faction") == "True"),
        int(args.get("hl_blops") == "True"),
        int(args.get("ig_factions") == "True"),
        args.get("gui_alpha")
        ]
    conn, cur = connect_db()
    cur.execute(
        '''INSERT INTO stats (uuid, ipaddr, version, platform, chars, duration,
        sh_faction, hl_blops, ig_factions, gui_alpha)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
        record
        )
    conn.commit()
    conn.close()
    return "Success"


@app.route("/stats/")
@auth.login_required
def show_stats():
    """
    Gathers some basic data from the stats table of the PySpy database
    and presents it as a simple html.
    """
    try:
        conn, cur = connect_db()
    except Exception:
        conn.close()
        return "Cannot connect to Database"

    try:
        # Quick clean up in case people have been using Pyspy 0.2 and eralier
        cur.execute("UPDATE stats SET platform = 'Windows' WHERE platform = 'nt'")
        cur.execute("UPDATE stats SET platform = 'Darwin' WHERE platform = 'posix'")

        # 0. WEEKLY ACTIVE USERS
        cur.execute("SELECT COUNT(DISTINCT(uuid)) FROM stats WHERE time > DATE_SUB(CURDATE(), INTERVAL 7 DAY)")
        weekly_users = "{:,}".format(cur.fetchone()[0])

        # 1. DAILY ACTIVE USERS
        cur.execute("SELECT COUNT(DISTINCT(uuid)) FROM stats WHERE time > DATE_SUB(CURDATE(), INTERVAL 1 DAY)")
        daily_users = "{:,}".format(cur.fetchone()[0])

        # 2. WEEKLY ACTIVE USERS BY PLATFORM
        cur.execute("SELECT platform, COUNT(DISTINCT(uuid)) FROM stats WHERE time > DATE_SUB(CURDATE(), INTERVAL 7 DAY) GROUP BY platform")
        weekly_users_os = cur.fetchall()
        if len(weekly_users_os) < 3:
            weekly_users_os = list(weekly_users_os)
            weekly_users_os.append(["", ""])

        # 3. TOTAL NUMBER OF SCANS
        cur.execute("SELECT COUNT(id) FROM stats")
        total_scans = "{:,}".format(cur.fetchone()[0])

        # 4. TOTAL NUMBER OF SCANS LAST 7 DAYS
        cur.execute("SELECT COUNT(id) FROM stats WHERE time > DATE_SUB(CURDATE(), INTERVAL 7 DAY)")
        scans_last_week = "{:,}".format(cur.fetchone()[0])

        # 5. TOTAL NUMBER OF SCANS LAST 1 DAYS
        cur.execute("SELECT COUNT(id) FROM stats WHERE time > DATE_SUB(CURDATE(), INTERVAL 1 DAY)")
        scans_last_day = "{:,}".format(cur.fetchone()[0])

        # 6. TOTAL NUMBER OF CHARACTERS ANALYSED
        cur.execute("SELECT SUM(chars) FROM stats")
        characters_analysed = "{:,}".format(cur.fetchone()[0])

    except Exception as e:
        conn.close()
        return "Databse Query Error"

    try:
        output = '''
            <!DOCTYPE html><html><head>
            <style>
                body {8} background-color: black; color: gainsboro; font-family: "Courier New", Courier, monospace; font-size: small; {9}
                h1 {8} font-weight: lightest; font-size: medium; {9}
                h2 {8} font-weight: lightest; font-size: small; {9}
                table, td {8} border: 0px; font-family: "Courier New", Courier, monospace; font-size: small; {9}
                td {8} text-align: left; vertical-align: top; {9}
                td + td {8} text-align: right; vertical-align: top; {9}
            </style></head><body>

            <h1>PYSPY DATABASE STATISTICS<h1>
            <table>
            <tr><td colspan="2"><h2> General </h2></td></tr>
                <tr><td>Total number of scans:</td> <td> {3} </td></tr>
                <tr><td>Scans in last 7 days:</td> <td> {4} </td></tr>
                <tr><td>Scans in last 24 hours:</td> <td> {5} </td></tr>
                <tr><td>Total number of characters analysed:</td> <td> {6} </td></tr>

            <tr><td colspan="2"><h2> Active Users </h2></td></tr>
                <tr><td>Last 7 days:</td> <td> {0} </td></tr>
                <tr><td>Last 24 hours:</td> <td> {1} </td></tr>
                <tr><td>Last 7 days by operating system:</td>
                    <td>
                        <table>
                            <tr><td> {2[0][0]} </td><td> {2[0][1]} </td></tr>
                            <tr><td> {2[1][0]} </td><td> {2[1][1]} </td></tr>
                            <tr><td> {2[2][0]} </td><td> {2[2][1]} </td></tr>
                        </table>
                    </td>
                </tr>
            <tr></tr>
            <tr><td colspan="2">EVE Time: {7} </td></tr>
            </table>
            </body></html>

        '''.format(
            weekly_users,
            daily_users,
            weekly_users_os,
            total_scans,
            scans_last_week,
            scans_last_day,
            characters_analysed,
            time.asctime(time.gmtime()),
            "{",
            "}"
            )
        message = output
    except:
        message = "Error in parsing output html."

    finally:
        conn.close()
        return message


@app.route("/upload/v1/", methods=["POST"])
@auth.login_required
def update_pyspy_db():
    """
    Receives character intel data as JSON string via POST requests and
    depending on whether the data relates to victims or attackers,
    prepares the relevant INSERT SQL statements and writes them to `intel`
    table of the PySpy database.

    :return: Returns number of affected rows as integer if everything
    went to plan (1 per new rows inserted, 2 per exisitng row updated).
    Otherwise, returns an error message as string.
    """
    if request.is_json:
        records = json.loads(request.json)
    else:
        return "Bad Request, no JSON found."

    query_values_list = []
    # If the upload is victim data, it will contain key `covertProb``
    if "losses" in records[0]:
        query_string = '''INSERT INTO intel
                (`character_id`,
                `losses`,
                `last_loss_date`,
                `covert_prob`,
                `normal_prob`,
                `last_cov_ship`,
                `last_norm_ship`,
                `abyssal_losses`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                losses=VALUES(losses),
                last_loss_date=VALUES(last_loss_date),
                covert_prob=VALUES(covert_prob),
                normal_prob=VALUES(normal_prob),
                last_cov_ship=VALUES(last_cov_ship),
                last_norm_ship=VALUES(last_norm_ship),
                abyssal_losses=VALUES(abyssal_losses)'''
        for record in records:
            try:
                query_values_list.append([
                    record["victim_id"],
                    record["losses"],
                    record["last_loss_date"],
                    record["covert_prob"],
                    record["normal_prob"],
                    record["last_cov_ship"],
                    record["last_norm_ship"],
                    record["abyssal_losses"],
                    ])
            except KeyError: # If any dictionary key is missing
                return "Bad JSON Data missing mandatory key."

    # If the upload is attacker data, it will contain key`avgAttackers`
    elif "kills" in records[0]:
        query_string = '''INSERT INTO intel
                (`character_id`,
                `kills`,
                `last_kill_date`,
                `avg_attackers`)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                kills=VALUES(kills),
                last_kill_date=VALUES(last_kill_date),
                avg_attackers=VALUES(avg_attackers)'''
        for record in records:
            try:
                query_values_list.append([
                    record["attacker_id"],
                    record["kills"],
                    record["last_kill_date"],
                    record["avg_attackers"],
                    ])
            except KeyError: # If any dictionary key is missing
                return "Bad JSON Data missing mandatory key."

    # DO NOT CONNECT TO DATABASE WHEN RUN LOCALLY FOR TESTING
    if __name__ != '__main__':
        conn, cur = connect_db()
        try:
            affected_rows = cur.executemany(query_string, query_values_list)
            message = str(affected_rows)
        except Exception as e:
            message = "Database Error: " + str(e)
        finally:
            conn.commit()
            conn.close()
            return message
    else:
        return "Test run ok!"


@app.route("/character_intel/v1/", methods=["POST"])
def retrieve_character_intel():
    """
    Take a list of character ids, delivered as JSON and return relevant
    data contained in the `intel` table.

    :return: Returns a dictionary as JSON string.
    """
    pyspy_useragent = "PySpy, Author: White Russsian, https://github.com/WhiteRusssian/PySpy"
    if request.user_agent.string != pyspy_useragent:
        return "Bad Request, not authorised."

    if request.is_json:
        records = request.json
    else:
        return "Bad Request, no JSON found."
    try:
        character_ids = records["character_ids"]
    except KeyError:
        return "Bad JSON Data, could not find character_ids."

    if len(character_ids) > 0:
        query_string = '''SELECT character_id, last_loss_date , last_kill_date,
            avg_attackers, covert_prob, normal_prob, last_cov_ship, last_norm_ship,
            abyssal_losses, losses, kills
            FROM intel WHERE character_id
            IN (%s)''' % (', '.join(['%s'] * len(character_ids)))
    else:
        return "Bad JSON Data, could not find character_ids."

    # DO NOT CONNECT TO DATABASE WHEN RUN LOCALLY FOR TESTING
    if __name__ != '__main__':
        conn, cur = connect_db()
        try:
            cur.execute(query_string, character_ids)
            results = cur.fetchall()
            # Convert results into adictionary with column headers as keys and
            # serialize it into a JSON string
            message = json.dumps([
                dict(zip([key[0] for key in cur.description], row)) for row in results
                ])
        except:
            message = "Database Error"
        finally:
            conn.close()
            return message


# ======================================================================
# FOR LOCAL TESTING PURPOSES
# ======================================================================
if __name__ == '__main__':

    import threading
    server_thread = threading.Thread(target=app.run)
    server_thread.start()

    import requests

    def test_upload():
        url = "http://localhost:5000" + "/upload/v1/"

        # Just some random json data
        json_data = [
            {"lastActivityDate": 20180808, "lossCnt" : 2, "abyssalCnt" : 2, "character_id" : 90000059, "covertProb" : 0, "normalProb" : 0 },
            {"lastActivityDate": 20180808, "lossCnt" : 11, "abyssalCnt" : 6, "character_id" : 90000154, "covertProb" : 0.5, "normalProb" : 0 },
            {"lastActivityDate": 20180808, "lossCnt" : 2, "abyssalCnt" : 4, "character_id" : 90000184, "covertProb" : 0.3, "normalProb" : 0 },
            {"lastActivityDate": 20180808, "lossCnt" : 1, "abyssalCnt" : 0, "character_id" : 90000292, "covertProb" : 0, "normalProb" : 0 },
            {"lastActivityDate": 20180808, "lossCnt" : 4, "abyssalCnt" : 0, "character_id" : 90000327, "covertProb" : 0, "normalProb" : 0.3 }
            ]

        r = requests.post(url, json=json_data)
        response = r.text
        print(r.status_code)
        print(response)

    def test_get_intel():
        url = "http://localhost:5000" + "/character_intel/" + "v1/"
        headers = {
            "Accept-Encoding": "gzip",
            "User-Agent": "PySpy, Author: White Russsian, https://github.com/WhiteRusssian/PySpy"
            }
        # Just some random json data
        character_ids = {"character_ids": (90000059, 90000154,90000184)}
        # character_ids = json.dumps(character_ids)
        r = requests.post(url, headers=headers, json=character_ids)
        response = r.text
        print(r.status_code)
        print(response)

    test_get_intel()
