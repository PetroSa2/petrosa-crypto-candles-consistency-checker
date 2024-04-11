"""
This script runs a consistency checker for the PETROSA database.
It continuously checks the database for consistency and logs any errors encountered.

Author: @yurisa2
Date: 2023-08-10
"""

import logging
from datetime import datetime

from app import checker_sql
from app import checker_mg

from app import variables

logging.basicConfig(level=variables.LOG_LEVEL)

start_datetime = datetime.utcnow()

logging.warning('Starting the checker')

if variables.ENABLE_SQL:
    checker_sql = checker_sql.PETROSAdbchecker()
    
if variables.ENABLE_MG:
    checker_mg = checker_mg.PETROSAdbchecker()


while True:
    try:

        if variables.ENABLE_SQL:
            checker_sql.check_db()
            
        if variables.ENABLE_MG:
            checker_mg.check_db()

    except Exception as e:
        logging.error(e)
        pass
