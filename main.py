import config
from utils.Database import Database

db = Database()
db.connect(config.db_host, config.db_user, config.db_pass, config.db_name)
db.assign_job_to_worker(config.worker_id)
job = db.get_next_processing_job_by_worker(config.worker_id)
print(job)
