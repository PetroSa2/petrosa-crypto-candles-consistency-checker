import datetime
import logging
import random
import time
import retry
from petrosa.database import mongo

from app.variables import (
    METER,
    SVC,
    MAX_CHECKING_TIMES,
    DEFAULT_CHECKING_BATCH_SIZE,
    MAX_CHECKING_BATCH_SIZE,
    TRACER,
    UNI_ID
)


class PETROSAdbchecker(object):
    @TRACER.start_as_current_span(name="init")
    def __init__(self) -> None:
        self.client_mg = mongo.get_client()
        self.backfill_col = self.client_mg.petrosa_crypto["backfill"]
        self.consistency_counter_found = METER.create_counter(
            SVC
            + ".found.count."
            + UNI_ID,
            unit="1",
            description="Items found count",
        )
        self.consistency_counter_m5 = METER.create_counter(
            SVC + ".found.m5." + UNI_ID,
            unit="1",
            description="Items found m5",
        )
        self.consistency_counter_m15 = METER.create_counter(
            SVC + ".found.m15." + UNI_ID,
            unit="1",
            description="Items found m15",
        )
        self.consistency_counter_m30 = METER.create_counter(
            SVC + ".found.m30." + UNI_ID,
            unit="1",
            description="Items found m30",
        )
        self.consistency_counter_h1 = METER.create_counter(
            SVC + ".found.h1." + UNI_ID,
            unit="1",
            description="Items found h1",
        )
        self.consistency_counter_wrong_count = METER.create_counter(
            SVC
            + ".wrong.count."
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
            SVC + ".ok." + UNI_ID,
            unit="1",
            description="consistency_counter_ok",
        )

    @TRACER.start_as_current_span(name="check_db")
    @retry.retry(tries=5, backoff=2, logger=logging.getLogger(__name__))
    def check_db(self):
        found_list = self.backfill_col.aggregate(
            [
                {
                    "$match": {
                        "checked": False,
                        "checking_times": {"$lt": MAX_CHECKING_TIMES},
                        "day": {"$lt": datetime.datetime.today().strftime("%Y-%m-%d")},
                    }
                },
                {"$sample": {"size": MAX_CHECKING_BATCH_SIZE}},
            ]
        )

        found_list = list(found_list)
        if len(found_list) > DEFAULT_CHECKING_BATCH_SIZE:
            found_list = random.sample(found_list, DEFAULT_CHECKING_BATCH_SIZE)
            self.consistency_counter_found.add(len(found_list))
        else:
            logging.debug(
                "we got less then DEFAULT_CHECKING_BATCH_SIZE: " + str(len(found_list))
            )
            found_list = self.backfill_col.aggregate(
                [
                    {
                        "$match": {
                            "checked": False,
                            "checking_times": {"$lt": MAX_CHECKING_TIMES},
                            "day": {
                                "$lt": datetime.datetime.today().strftime("%Y-%m-%d")
                            },
                        }
                    },
                    {"$sample": {"size": MAX_CHECKING_BATCH_SIZE}},
                ]
            )
            found_list = list(found_list)
            found_list = random.sample(found_list, len(found_list))

        if len(found_list) == 0:
            logging.debug("Found nothin, suspicious. Waiting and then 333")
            time.sleep(10)
            found_list = self.backfill_col.aggregate(
                [
                    {
                        "$match": {
                            "day": {
                                "$lt": datetime.datetime.today().strftime("%Y-%m-%d")
                            }
                        }
                    },
                    {"$sample": {"size": 333}},
                ]
            )
            found_list = list(found_list)
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

            day_start = datetime.datetime.fromisoformat(found["day"])
            day_end = day_start + datetime.timedelta(days=1)

            candles_col = self.client_mg.petrosa_crypto[check_col]

            candles_found = candles_col.aggregate(
                [
                    {
                        "$match": {
                            "ticker": found["symbol"],
                            "datetime": {"$gte": day_start, "$lt": day_end},
                        }
                    },
                    {"$count": "ticker_count"},
                ]
            )

            candles_found_list = list(candles_found)
            if len(candles_found_list) > 0:
                candles_found = candles_found_list[0]["ticker_count"]
            else:
                candles_found = 0

            if count_check == candles_found:
                # logging.warning(found, ' OK')
                self.backfill_col.update_one(
                    {"_id": found["_id"]},
                    {
                        "$set": {
                            "state": 1,
                            "checked": True,
                            "last_checked": datetime.datetime.now(),
                        }
                    },
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
                    # logging.info('I found it but will increase cheking_times')

                    ck_times = found["checking_times"] + 1
                    self.backfill_col.update_one(
                        {"_id": found["_id"]},
                        {
                            "$set": {
                                "state": 0,
                                "checked": False,
                                "checking_times": ck_times,
                                "last_checked": datetime.datetime.now(),
                            }
                        },
                    )

                elif "checking_times" not in found:
                    logging.debug("There is not checking times bro")
                    logging.debug(found)

                    self.backfill_col.update_one(
                        {"_id": found["_id"]},
                        {
                            "$set": {
                                "state": 0,
                                "checked": False,
                                "checking_times": 1,
                                "last_checked": datetime.datetime.now(),
                            }
                        },
                    )
