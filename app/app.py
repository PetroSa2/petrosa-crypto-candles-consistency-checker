"""
This script runs a consistency checker for the PETROSA database.
It continuously checks the database for consistency and logs any errors encountered.

Author: @yurisa2
Date: 2023-08-10
"""

import logging
from datetime import datetime
import threading
import time

from app import checker_sql
from app import checker_mg

from app import variables

logging.basicConfig(level=variables.LOG_LEVEL)

start_datetime = datetime.utcnow()

logging.warning('Starting the checker')

if variables.CONSISTENCY_CHECKER_ENABLE_SQL:
    checker_sql = checker_sql.PETROSAdbchecker()
    th_checker_sql = threading.Thread(target=checker_sql.run_forever, name="th_checker_sql")
    th_checker_sql.start()


if variables.CONSISTENCY_CHECKER_ENABLE_MG:
    checker_mg = checker_mg.PETROSAdbchecker()
    th_checker_mg = threading.Thread(target=checker_mg.run_forever, name="th_checker_mg")
    th_checker_mg.start()


while True:
    try:

        if variables.CONSISTENCY_CHECKER_ENABLE_SQL:
            if not th_checker_sql.is_alive():
                logging.warning('Restarting the checker for SQL')
                threading.Thread(
                    target=checker_sql.run_forever, name="th_checker_sql"
                ).start()

        if variables.CONSISTENCY_CHECKER_ENABLE_MG:
            if not th_checker_mg.is_alive():
                logging.warning('Restarting the checker for MG')
                threading.Thread(
                    target=checker_mg.run_forever, name="th_checker_sql"
                ).start()

        time.sleep(variables.CHECKER_SLEEP_TIME)
    except Exception as e:
        logging.error(e)
        pass
