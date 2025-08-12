import os
import time
import logging
import traceback
from functools import wraps
from dotenv import load_dotenv
from slack_sdk.webhook import WebhookClient

# 🔄 Load .env
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# Simulated error
class FakeSparkError(Exception): pass

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

def retry_with_slack(max_retries=3, base_delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < max_retries:
                try:
                    return func(*args, **kwargs)
                except FakeSparkError as e:
                    attempt += 1
                    wait = base_delay * (2 ** (attempt - 1))
                    logging.warning(f"Attempt {attempt} failed: {e}")
                    if attempt == max_retries:
                        msg = f"❌ All {max_retries} retries FAILED for `{func.__name__}`.\nError: {e}"
                        logging.error(msg)
                        traceback.print_exc()
                        send_slack_alert(msg)
                        raise
                    time.sleep(wait)
        return wrapper
    return decorator

# 🧪 Test function that fails
@retry_with_slack(max_retries=3, base_delay=1)
def test_pipeline_step():
    raise FakeSparkError("This simulates a Spark or GCS failure.")

test_pipeline_step()
