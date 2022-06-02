import config
import sys
import importlib
from utils.Database import Database


db = Database()
db.connect(config.db_host, config.db_user, config.db_pass, config.db_name)

job = db.get_next_processing_job_by_worker(config.worker_id)
if job is None:
    db.assign_job_to_worker(config.worker_id)
    job = db.get_next_processing_job_by_worker(config.worker_id)

if job is None:
    sys.exit()

print(job)

module = importlib.import_module("backtesting." + job['strategy'])
class_ = getattr(module, job['strategy'])
test = class_(**job['params'])
test.test()
print(test.get_groupby_status())
print(f"Init exexution time {test.get_init_execution_time()}")
print(f"Test execution time {test.get_test_execution_time()}")
