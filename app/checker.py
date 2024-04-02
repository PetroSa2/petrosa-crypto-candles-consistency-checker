import datetime
import logging
import os
import random
import time
import retry
from opentelemetry import trace

from opentelemetry import metrics
from petrosa.database import sql

tracer = trace.get_tracer(
    os.getenv("OTEL_SERVICE_NAME", "no-service-name"), os.getenv("VERSION", "0.0.0")
)


if os.getenv("MARKET", "crypto") == "crypto":
    os.environ["MYSQL_USER"] = os.getenv("MYSQL_CRYPTO_USER")
    os.environ["MYSQL_PASSWORD"] = os.getenv("MYSQL_CRYPTO_PASSWORD")
    os.environ["MYSQL_SERVER"] = os.getenv("MYSQL_CRYPTO_SERVER")
    os.environ["MYSQL_DB"] = os.getenv("MYSQL_CRYPTO_DB")


MAX_CHECKING_TIMES = 1000
DEFAULT_CHECKING_BATCH_SIZE = 1000
MAX_CHECKING_BATCH_SIZE = 3000

meter = metrics.get_meter("global.meter")

uni_id = str(random.randint(1000, 9999))


class PETROSAdbchecker(object):
    @tracer.start_as_current_span(name="init")
    def __init__(self) -> None:
        self.consistency_counter_found = meter.create_counter(
            os.getenv("OTEL_SERVICE_NAME", "no-service-name")
            + ".found.count."
            + uni_id,
            unit="1",
            description="Items found count",
        )
        self.consistency_counter_m5 = meter.create_counter(
            os.getenv("OTEL_SERVICE_NAME", "no-service-name") + ".found.m5." + uni_id,
            unit="1",
            description="Items found m5",
        )
        self.consistency_counter_m15 = meter.create_counter(
            os.getenv("OTEL_SERVICE_NAME", "no-service-name") + ".found.m15." + uni_id,
            unit="1",
            description="Items found m15",
        )
        self.consistency_counter_m30 = meter.create_counter(
            os.getenv("OTEL_SERVICE_NAME", "no-service-name") + ".found.m30." + uni_id,
            unit="1",
            description="Items found m30",
        )
        self.consistency_counter_h1 = meter.create_counter(
            os.getenv("OTEL_SERVICE_NAME", "no-service-name") + ".found.h1." + uni_id,
            unit="1",
            description="Items found h1",
        )
        self.consistency_counter_wrong_count = meter.create_counter(
            os.getenv("OTEL_SERVICE_NAME", "no-service-name")
            + ".wrong.count."
            + uni_id,
            unit="1",
            description="consistency_counter_wrong_count",
        )
        self.consistency_counter_exhausted = meter.create_counter(
            os.getenv("OTEL_SERVICE_NAME", "no-service-name") + ".exh.tries." + uni_id,
            unit="1",
            description="consistency_counter_exhausted",
        )
        self.consistency_counter_ok = meter.create_counter(
            os.getenv("OTEL_SERVICE_NAME", "no-service-name") + ".ok." + uni_id,
            unit="1",
            description="consistency_counter_ok",
        )

    @tracer.start_as_current_span(name="check_db")
    @retry.retry(tries=5, backoff=2, logger=logging.getLogger(__name__))
    def check_db(self):
        logging.info("Checking DB")
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
            logging.info(
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
            logging.info("Found nothin, suspicious. Waiting and then 333")
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

                logging.info("that one is ok.")
                self.consistency_counter_ok.add(1)

            else:
                msg = "Thats Wrong, found this much: " + str(candles_found)
                logging.info(msg)
                logging.info(found)
                self.consistency_counter_wrong_count.add(1)

                if (
                    "checking_times" in found
                    and found["checking_times"] >= MAX_CHECKING_TIMES
                ):
                    logging.info("Exhausted tentatives")
                    logging.info(found)
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
                    logging.info("I found it but will increase cheking_times")

                    ck_times = found["checking_times"] + 1
                    logging.info("checking_times: " + str(ck_times))
                    sql.run_generic_sql(
                        "update backfill set state = 0, checked = 0, checking_times = "
                        + str(ck_times)
                        + ", last_checked = now() where id = "
                        + str(found["id"])
                    )
