from datetime import timedelta
from datetime import datetime

start_date = (datetime(2023, 6, 20, 0, 0, 0))
default_args = {
    'owner': 'zhaoyutim',
    'start_date': start_date,
    'depends_on_past': False,
    'email': ['zhaoyutim@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': timedelta(days=1),
}