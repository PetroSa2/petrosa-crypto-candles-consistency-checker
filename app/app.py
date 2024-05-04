"""
This script runs a consistency checker for the PETROSA database.
It continuously checks the database for consistency and logs any errors encountered.

Author: @yurisa2
Date: 2023-08-10
"""

import logging
from datetime import datetime
import threading

from app import checker_sql
from app import checker_mg

from app import variables

logging.basicConfig(level=variables.LOG_LEVEL)

start_datetime = datetime.utcnow()

logging.warning('Starting the checker')

if variables.CONSISTENCY_CHECKER_ENABLE_SQL:
    checker_sql = checker_sql.PETROSAdbchecker()
    threading.Thread(target=checker_sql.run_forever, name="th_checker_sql").start()


if variables.CONSISTENCY_CHECKER_ENABLE_MG:
    checker_mg = checker_mg.PETROSAdbchecker()
    threading.Thread(target=checker_mg.run_forever, name="th_checker_mg").start()


while True:
    try:

        if variables.CONSISTENCY_CHECKER_ENABLE_SQL:
            if not threading.Thread(name="th_checker_sql").is_alive():
                threading.Thread(
                    target=checker_sql.run_forever, name="th_checker_sql"
                ).start()

        if variables.CONSISTENCY_CHECKER_ENABLE_MG:
            if not threading.Thread(name="th_checker_mg").is_alive():
                threading.Thread(
                    target=checker_mg.run_forever, name="th_checker_sql"
                ).start()
    except Exception as e:
        logging.error(e)
        pass
