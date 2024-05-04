import datetime
import logging
import random
import time
import retry
from petrosa.database import sql

from app.variables import (
    METER,
    SVC,
    MAX_CHECKING_TIMES,
    DEFAULT_CHECKING_BATCH_SIZE,
    MAX_CHECKING_BATCH_SIZE,
    TRACER,
    UNI_ID,
)

class PETROSAdbchecker(object):
    @TRACER.start_as_current_span(name="init")
    def __init__(self) -> None:
        self.consistency_counter_found = METER.create_counter(
            SVC + ".sql.found.count."
            + UNI_ID,
            unit="1",
            description="Items found count",
        )
        self.consistency_counter_m5 = METER.create_counter(
            SVC + ".sql.found.m5." + UNI_ID,
            unit="1",
            description="Items found m5",
        )
        self.consistency_counter_m15 = METER.create_counter(
            SVC + ".sql.found.m15." + UNI_ID,
            unit="1",
            description="Items found m15",
        )
        self.consistency_counter_m30 = METER.create_counter(
            SVC + ".sql.found.m30." + UNI_ID,
            unit="1",
            description="Items found m30",
        )
        self.consistency_counter_h1 = METER.create_counter(
            SVC + ".sql.found.h1." + UNI_ID,
            unit="1",
            description="Items found h1",
        )
        self.consistency_counter_wrong_count = METER.create_counter(
            SVC             + ".sql.wrong.count."
            + UNI_ID,
            unit="1",
            description="consistency_counter_wrong_count",
        )
        self.consistency_counter_exhausted = METER.create_counter(
            SVC + ".exh.tries." + UNI_ID,
            unit="1",
            description="consistency_counter_exhausted",
        )
        self.consistency_counter_ok = METER.create_counter(
            SVC + ".sql.ok." + UNI_ID,
            unit="1",
            description="consistency_counter_ok",
        )


    @TRACER.start_as_current_span(name="check_db")
    @retry.retry(tries=5, backoff=2, logger=logging.getLogger(__name__))
    def check_db(self):
        """
        Check the database for consistency.

        This method retrieves a list of records from the 'backfill' table that meet certain criteria,
        such as being unchecked, having a limited number of checking times, and having a day earlier than today.
        It then performs consistency checks on each record by counting the number of candles in the corresponding
        table for the specified ticker and day. If the count matches the expected count based on the period,
        the record is marked as checked and the consistency counter is incremented. Otherwise, the record is
        marked as unchecked and the checking times is increased. If the checking times exceeds the maximum
        allowed value, the record is marked as exhausted.

        Returns:
            None
        """
        logging.debug("Checking DB")
        found_list = sql.run_generic_sql(
            "select * from backfill where checked = 0 and checking_times < "
            + str(MAX_CHECKING_TIMES)
            + " and day < '"
            + datetime.datetime.today().strftime("%Y-%m-%d")
            + "' ORDER BY RAND() limit "
            + str(MAX_CHECKING_BATCH_SIZE)
        )

        if len(found_list) > DEFAULT_CHECKING_BATCH_SIZE:
            found_list = random.sample(found_list, DEFAULT_CHECKING_BATCH_SIZE)
            self.consistency_counter_found.add(len(found_list))
        else:
            logging.debug(
                "we got less then DEFAULT_CHECKING_BATCH_SIZE: " + str(len(found_list))
            )
            found_list = sql.run_generic_sql(
                "select * from backfill where checked = 0 and checking_times < "
                + str(MAX_CHECKING_TIMES)
                + " and day < '"
                + datetime.datetime.today().strftime("%Y-%m-%d")
                + "' ORDER BY RAND() limit "
                + str(MAX_CHECKING_BATCH_SIZE)
            )

            found_list = random.sample(found_list, len(found_list))

        if len(found_list) == 0:
            logging.debug("Found nothin, suspicious. Waiting and then 333")
            time.sleep(10)

            found_list = sql.run_generic_sql(
                "select * from backfill where day < '"
                + datetime.datetime.today().strftime("%Y-%m-%d")
                + "' ORDER BY RAND() limit "
                + str(333)
            )

            found_list = random.sample(found_list, len(found_list))

        for found in found_list:
            col_name = ""
            count_check = 0

            if found["period"] == "5m":
                col_name = "m5"
                count_check = 288
                self.consistency_counter_m5.add(1)
            if found["period"] == "15m":
                col_name = "m15"
                count_check = 96
                self.consistency_counter_m15.add(1)
            if found["period"] == "30m":
                col_name = "m30"
                count_check = 48
                self.consistency_counter_m30.add(1)
            if found["period"] == "1h":
                col_name = "h1"
                count_check = 24
                self.consistency_counter_h1.add(1)

            check_col = "candles_" + col_name

            day_start = datetime.datetime.fromisoformat(str(found["day"]))
            day_end = day_start + datetime.timedelta(days=1)

            candles_found_list = sql.run_generic_sql(
                f"""SELECT COUNT(*) AS ticker_count
                    FROM {check_col}
                    WHERE ticker = '{found["symbol"]}'
                    AND datetime >= '{str(day_start)}'
                    AND datetime < '{str(day_end)}';
                    """
            )

            if len(candles_found_list) > 0:
                candles_found = candles_found_list[0]["ticker_count"]
            else:
                candles_found = 0

            if count_check == candles_found:
                # logging.warning(found, ' OK')

                sql.run_generic_sql(
                    "update backfill set state = 1, checked = 1, last_checked = now() where id = "
                    + str(found["id"])
                )

                logging.debug("that one is ok.")
                self.consistency_counter_ok.add(1)

            else:
                msg = "Thats Wrong, found this much: " + str(candles_found)
                logging.debug(msg)
                logging.debug(found)
                self.consistency_counter_wrong_count.add(1)

                if (
                    "checking_times" in found
                    and found["checking_times"] >= MAX_CHECKING_TIMES
                ):
                    logging.debug("Exhausted tentatives")
                    logging.debug(found)
                    self.consistency_counter_exhausted.add(1)

                    pass
                    # self.backfill_col.update_one(
                    #     {"_id": found['_id']},{
                    #         "$set": {"state": 1, "checked": True,
                    #         "last_checked": datetime.datetime.now()}}
                    #     )

                elif (
                    "checking_times" in found
                    and found["checking_times"] < MAX_CHECKING_TIMES
                ):
                    logging.debug("I found it but will increase cheking_times")

                    ck_times = found["checking_times"] + 1
                    logging.debug("checking_times: " + str(ck_times))
                    sql.run_generic_sql(
                        "update backfill set state = 0, checked = 0, checking_times = "
                        + str(ck_times)
                        + ", last_checked = now() where id = "
                        + str(found["id"])
                    )


    @TRACER.start_as_current_span(name="run_forever")
    def run_forever(self):
            """
            Continuously runs the consistency checker.

            This method runs an infinite loop that repeatedly calls the `check_db` method to perform consistency checks on the database.
            If an exception occurs during the check, it is logged and the loop continues.

            Returns:
                None
            """
            while True:
                try:
                    self.check_db()
                    time.sleep(3)
                except Exception as e:
                    logging.error(e)
                    pass
