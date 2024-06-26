import os
from opentelemetry import metrics
from opentelemetry import trace
import random
from dotenv import load_dotenv

load_dotenv()

CONSISTENCY_CHECKER_ENABLE_MG = bool(os.getenv("CONSISTENCY_CHECKER_ENABLE_MG", 0))
CONSISTENCY_CHECKER_ENABLE_SQL = bool(os.getenv("CONSISTENCY_CHECKER_ENABLE_SQL", 1))

CHECKER_SLEEP_TIME = int(os.getenv("CHECKER_SLEEP_TIME", 60))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

TRACER = tracer = trace.get_tracer(
    os.getenv("OTEL_SERVICE_NAME", "no-service-name"), os.getenv("VERSION", "0.0.0")
)

METER = metrics.get_meter("global.meter")
OTEL_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "default-service-name")
SVC = OTEL_SERVICE_NAME

if os.getenv("MARKET", "crypto") == "crypto":
    os.environ["MYSQL_USER"] = os.getenv("MYSQL_CRYPTO_USER")
    os.environ["MYSQL_PASSWORD"] = os.getenv("MYSQL_CRYPTO_PASSWORD")
    os.environ["MYSQL_SERVER"] = os.getenv("MYSQL_CRYPTO_SERVER")
    os.environ["MYSQL_DB"] = os.getenv("MYSQL_CRYPTO_DB")

MAX_CHECKING_TIMES = 1000
DEFAULT_CHECKING_BATCH_SIZE = 1000
MAX_CHECKING_BATCH_SIZE = 3000

UNI_ID = str(random.randint(1000, 9999))
