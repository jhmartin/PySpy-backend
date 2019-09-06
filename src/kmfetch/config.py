# !/usr/local/bin/python3.6
# MIT licensed
# Copyright (c) 2018 White Russsian
# Github: <https://github.com/Eve-PySpy/PySpy>**********************
""" Define a few paths and constants used throughout other modules."""
# **********************************************************************
import logging.config
import logging
import os
# cSpell Checker - Correct Words****************************************
# // cSpell:words MEIPASS, datefmt, russsian, pyinstaller, posix, pyspy
# **********************************************************************
Logger = logging.getLogger(__name__)
# Example call: Logger.info("Something badhappened", exc_info=True) ****

DATA_PATH = os.path.expanduser("/")
# DATA_PATH = os.path.expanduser("/Volumes/Media/sqlite3db")

LOG_FILE = os.path.join(".", "kmfetch.log")

# Auth Data for the upload
PYSPY_USER = os.environ.get("PYSPY_USER")
PYSPY_PWD = os.environ.get("PYSPY_PWD")

# URL where the Flask is hosted
UPLOAD_URL = "pyspy.pythonanywhere.com"

# IP of the Mongo Instance
MONGO_SERVER_IP = "127.0.0.1"

# IF time on check between START_TIME and END_TIME script will be excecuted; START_TIME and END_TIME are in 24h format
# SLEEP_TIME decides how long the script will sleep till the next time check
START_TIME = 5  # hours
END_TIME = 6  # hours
SLEEP_TIME = 3600  # seconds

# Logging setup
''' 
For each module that requires logging, make sure to import modules
logging and this config. Then get a new logger at the beginning
of the module like this: "Logger = logging.getLogger(__name__)" and
produce log messages like this: "Logger.error("text", exc_info=True)"
'''
LOG_DETAIL = 'DEBUG'

LOG_DICT = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s]: %(message)s',
            'datefmt': '%d-%b-%Y %I:%M:%S %p'
        },
    },
    'handlers': {
        'stream_handler': {
            'level': 'DEBUG',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
        },
        'file_handler': {
            'level': 'DEBUG',
            'filename': LOG_FILE,
            'class': 'logging.FileHandler',
            'formatter': 'standard'
        },
        'timed_rotating_file_handler': {
            'level': 'DEBUG',
            'filename': LOG_FILE,
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'when': 'D',
            'interval': 7,  # Log file rolling over every week
            'backupCount': 1,
            'formatter': 'standard'
        },
    },
    'loggers': {
        '': {
            'handlers': ['timed_rotating_file_handler', 'stream_handler'],
            'level': 'INFO',
            'propagate': True
        },
    }
}
logging.config.dictConfig(LOG_DICT)
