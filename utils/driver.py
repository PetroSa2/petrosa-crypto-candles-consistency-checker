import datetime
from petrosa.database import sql
import os

os.environ["MYSQL_USER"] = ""
os.environ["MYSQL_PASSWORD"] = ""
os.environ["MYSQL_SERVER"] = ""
os.environ["MYSQL_DB"] = ""


found = {"period": '1h', "symbol": 'BTCUSDT', "day": '2021-01-01'}

if(found['period'] == '5m'):
    col_name = 'm5'
    count_check = 288
if(found['period'] == '15m'):
    col_name = 'm15'
    count_check = 96
if(found['period'] == '30m'):
    col_name = 'm30'
    count_check = 48
if(found['period'] == '1h'):
    col_name = 'h1'
    count_check = 24


check_col = 'candles_' + col_name

day_start = datetime.datetime.fromisoformat(found['day'])
day_end = day_start + datetime.timedelta(days=1)

# candles_col = client_mg.petrosa_crypto[check_col]
# candles_found = candles_col.find({"ticker": found['symbol'],
#                                   "datetime": {"$gte": day_start, "$lt": day_end}})

candles_found = sql.run_generic_sql("select * from candles_h1 limit 1")[0]

print(candles_found)


candles_found = list(candles_found)
if(count_check == len(candles_found)):
    logging.warning(found, ' OK')
    col.update_one({"_id": found['_id']}, {"$set": {"checked": True}})
else:
    col.update_one({"_id": found['_id']}, {"$set": {"state": 0}})
