from apscheduler.schedulers.background import BackgroundScheduler
from email.utils import formatdate
from hmac_generator import get_signature
from data_analyst import preprocess_data
import signature_cache

def refresh_signature():
    date = formatdate(timeval=None, localtime=False, usegmt=True)
    signature = get_signature(date)
    signature_cache.date = date
    signature_cache.signature = signature
    print(f"Signature refreshed at {date}")

def start_refresh_signature_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(refresh_signature, "interval", minutes=15)
    refresh_signature()  # Initial call
    scheduler.start()


def start_preprocess_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(preprocess_data, "interval", hours=1)
    preprocess_data()
    scheduler.start()