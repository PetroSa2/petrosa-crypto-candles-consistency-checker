"""
This script runs a consistency checker for the PETROSA database.
It continuously checks the database for consistency and logs any errors encountered.

Author: @yurisa2
Date: 2023-08-10
"""

import logging
from datetime import datetime

from app import checker

logging.basicConfig(level=logging.INFO)

start_datetime = datetime.utcnow()

logging.warning('Starting the checker')
checker_instance = checker.PETROSAdbchecker()


while True:
    try:
        checker_instance.check_db()
    except Exception as e:
        logging.error(e)
        pass
