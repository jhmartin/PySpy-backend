# !/usr/local/bin/python3.6
'''
'''
# **********************************************************************
import os
import sqlite3
import logging
import config
# cSpell Checker - Correct Words****************************************
# // cSpell:words sqlite, DECLTYPES, isfile, pyspy, cyno
# **********************************************************************
Logger = logging.getLogger(__name__)
# Example call: Logger.info("Something badhappened", exc_info=True) ****


def connect_db():
    '''
    Check if SQLite DB already exists. If not creates a new DB file.

    Creates custom adapter for boolean data type (BOOL)

    :return: SQLite connection object
    '''

    DB_DIR = config.DATA_PATH
    # If script runs for the first time, may have to create the
    # subdirectory for the database file (DB_DIR).
    if not os.path.exists(DB_DIR):
        try:
            os.makedirs(DB_DIR)
        except Exception:
            Logger.error("Failed to create database directory", exc_info=True)
            raise Exception
    # Open database file or create a new file if it does not exist.
    try:
        db_file = os.path.join(DB_DIR, "pyspy.db")
        create_new_db = not os.path.isfile(db_file)
        con = sqlite3.connect(
            db_file,
            detect_types=sqlite3.PARSE_DECLTYPES
            )
        # Boolean adapter registration
        sqlite3.register_adapter(bool, int)
        sqlite3.register_converter("BOOL", lambda v: bool(int(v)))

        con.execute("PRAGMA journal_mode = TRUNCATE")
        prepare_tables(con)
        if create_new_db:
            print('prepped tables')
            prepare_indexes(con)
        return con
    except Exception:
        Logger.error("Could not connect to SQLite database.", exc_info=True)
        raise Exception


def prepare_tables(con):
    '''Create a few tables and views, unless they already exist. Do not close the
    connection as it will continue to be used by the calling
    function.'''
    con.execute(
        '''CREATE TABLE IF NOT EXISTS km_summary
        (killmail_id INT PRIMARY KEY, killmail_date INT, solar_system_id INT,
        abyssal BOOL, attacker_count INT, victim_id INT, ship_id INT,
        covert_cyno BOOL, normal_cyno BOOL)'''
        )
    con.execute(
        '''CREATE TABLE IF NOT EXISTS attackers
        (killmail_id INT, killmail_date INT, attacker_id INT, attacker_count INT,
        UNIQUE(killmail_id, attacker_id))'''
        )
    con.execute(
        '''CREATE VIEW IF NOT EXISTS view_cov_cyno AS
        SELECT victim_id, ship_id FROM km_summary
        WHERE covert_cyno=1 ORDER BY killmail_id DESC'''
        )
    con.execute(
        '''CREATE VIEW IF NOT EXISTS view_norm_cyno AS
        SELECT victim_id, ship_id FROM km_summary
        WHERE normal_cyno=1 ORDER BY killmail_id DESC'''
        )
    con.execute('DROP VIEW atk_sum')
    con.execute(
        '''CREATE VIEW atk_sum AS
        SELECT attackers.attacker_id AS character_id, count(attackers.killmail_id) AS kills,
        avg(attackers.attacker_count) AS avg_attackers,
        max(killmail_date) AS last_kill_date
        FROM attackers GROUP BY attacker_id'''
        )
    con.execute('DROP VIEW vic_sum')
    con.execute(
        '''CREATE VIEW vic_sum AS
        SELECT victim_id AS character_id, count(killmail_id) AS losses,
        total(covert_cyno)/count(killmail_id) AS covert_prob,
        total(normal_cyno)/count(killmail_id) AS normal_prob,
        sum(abyssal) AS abyssal_losses,
        (SELECT ship_id FROM view_cov_cyno
            WHERE view_cov_cyno.victim_id=km_summary.victim_id LIMIT 1)
            AS last_cov_ship,
        (SELECT ship_id FROM view_norm_cyno
            WHERE view_norm_cyno.victim_id=km_summary.victim_id LIMIT 1)
            AS last_norm_ship, max(killmail_date)
            AS last_loss_date
        FROM km_summary GROUP BY victim_id'''
        )
    con.commit()


def prepare_indexes(con):
    '''Create some useful indices, unless they already exist. Do not close the
    connection as it will continue to be used by the calling
    function.'''
    con.execute(
        '''CREATE INDEX IF NOT EXISTS atk1 ON attackers
        (attacker_id ASC, attacker_count ASC)'''
        )  # Used for attacker summary
    con.execute(
        '''CREATE INDEX IF NOT EXISTS atk2 ON attackers
        (attacker_id ASC, killmail_date ASC);'''
        )  # Used for touched_attackers
    con.execute(
        '''CREATE INDEX IF NOT EXISTS kms1 on km_summary
        (victim_id ASC, covert_cyno ASC)'''
        )  # Used for victim summary
    con.execute(
        '''CREATE INDEX IF NOT EXISTS kms2 on km_summary
        (victim_id ASC, normal_cyno ASC)'''
        )  # Used for victim summary
    con.execute(
        '''CREATE INDEX IF NOT EXISTS kms3 on km_summary
        (killmail_date ASC, killmail_id ASC)'''
        )  # Used in create_summaries

    con.commit()
