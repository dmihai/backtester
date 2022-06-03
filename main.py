import sys
import importlib
import psutil
import os

import config
import constants
from utils.Database import Database


def is_running(script):
    for q in psutil.process_iter():
        if q.name().startswith('python'):
            if len(q.cmdline()) > 1 and script in q.cmdline()[1] and q.pid != os.getpid():
                print("'{}' Process is already running".format(script))
                return True

    return False


if is_running('main.py'):
    sys.exit()

db = Database()
db.connect(config.db_host, config.db_user, config.db_pass, config.db_name)

job = db.get_next_processing_job_by_worker(config.worker_id, constants.version)
if job is None:
    db.assign_job_to_worker(config.worker_id, constants.version)
    job = db.get_next_processing_job_by_worker(
        config.worker_id, constants.version)

if job is None:
    sys.exit()

module = importlib.import_module("backtesting." + job['strategy'])
class_ = getattr(module, job['strategy'])

test = class_(**job['params'])
test.test()
results = test.get_results()
execution_time = test.get_init_execution_time() + test.get_test_execution_time()

db.save_job_results(job['id'], results)
db.finish_job_by_id(job['id'], execution_time)
