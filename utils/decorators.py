# utils/decorators.py

import os
import time
import logging
import traceback
from functools import wraps
from slack_sdk.webhook import WebhookClient

# Optional: load_dotenv() if needed
# from dotenv import load_dotenv
# load_dotenv()

# PySpark & GCP imports (optional fallback)
try:
    from pyspark.sql.utils import AnalysisException
except ImportError:
    AnalysisException = Exception

try:
    from google.api_core.exceptions import GoogleAPIError
except ImportError:
    GoogleAPIError = Exception

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ------------------------ #
# 🔁 Simple Decorators     #
# ------------------------ #

def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        logging.info(f"⏳ Starting '{func.__name__}'...")
        result = func(*args, **kwargs)
        end = time.time()
        logging.info(f"✅ Finished '{func.__name__}' in {end - start:.2f} seconds.")
        return result
    return wrapper

def log_inputs(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logging.info(f"📝 Called '{func.__name__}' with args: {args} kwargs: {kwargs}")
        return func(*args, **kwargs)
    return wrapper

def retry(max_retries=3, delay=2, exceptions=(Exception,)):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    logging.warning(f"⚠️ Attempt {attempt} failed for '{func.__name__}': {e}")
                    if attempt == max_retries:
                        logging.error(f"❌ All {max_retries} retries failed for '{func.__name__}'")
                        traceback.print_exc()
                        raise
                    time.sleep(delay)
        return wrapper
    return decorator

# ------------------------ #
# 🔁 Retry for Spark & GCS #
# ------------------------ #

def retry_spark_gcs(max_retries=3, delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < max_retries:
                try:
                    return func(*args, **kwargs)
                except (AnalysisException, GoogleAPIError) as e:
                    attempt += 1
                    logging.warning(
                        f"⚠️ Retry {attempt} for '{func.__name__}' due to: {type(e).__name__} - {e}"
                    )
                    if attempt == max_retries:
                        logging.error(
                            f"❌ All {max_retries} retries failed for '{func.__name__}'"
                        )
                        traceback.print_exc()
                        raise
                    time.sleep(delay)
        return wrapper
    return decorator

# ------------------------ #
# 🔁 Retry + Slack Alerts  #
# ------------------------ #

def send_slack_alert(message):
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if webhook_url:
        try:
            webhook = WebhookClient(webhook_url)
            response = webhook.send(text=message)
            if response.status_code != 200:
                logging.warning("Slack alert failed")
        except Exception as e:
            logging.warning(f"Slack alert error: {e}")
    else:
        logging.warning("No Slack webhook URL configured")

def retry_spark_gcs_with_alert(max_retries=3, base_delay=2, exception_types=(Exception,)):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < max_retries:
                try:
                    return func(*args, **kwargs)
                except exception_types as e:
                    attempt += 1
                    wait_time = base_delay * (2 ** (attempt - 1))
                    logging.warning(f"Attempt {attempt} failed for '{func.__name__}': {e}")
                    if attempt == max_retries:
                        msg = f"❌ All {max_retries} retries FAILED for `{func.__name__}`.\nError: {e}"
                        logging.error(msg)
                        traceback.print_exc()
                        send_slack_alert(msg)
                        raise
                    time.sleep(wait_time)
        return wrapper
    return decorator
