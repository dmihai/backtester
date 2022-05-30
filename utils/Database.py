import mysql.connector
import json


class Database:
    def __del__(self):
        self._conn.close()

    def connect(self, db_host, db_user, db_pass, db_name):
        self._conn = mysql.connector.connect(host=db_host,
                                             database=db_name,
                                             user=db_user,
                                             password=db_pass)

    def get_next_processing_job_by_worker(self, worker_id):
        cursor = self._conn.cursor()

        query = "SELECT id, strategy, strategy_version, asset, year, timeframe, params\
            FROM jobs\
            WHERE status=%s AND worker=%s\
            LIMIT 1"
        cursor.execute(query, ('processing', worker_id))

        result = cursor.fetchone()

        if result is None:
            return None

        params = json.loads(result[6])
        params['asset'] = result[3]
        params['year'] = result[4]
        params['timeframe'] = result[5]

        return {
            "id": result[0],
            "strategy": result[1],
            "params": params
        }

    def assign_job_to_worker(self, worker_id):
        cursor = self._conn.cursor()

        query = "UPDATE jobs\
            SET status = 'processing', worker = %s, last_update = now()\
            WHERE status = 'idle'"
        cursor.execute(query, (worker_id,))

        self._conn.commit()

    def finish_job_by_id(self, job_id, execution_time):
        cursor = self._conn.cursor()

        query = "UPDATE jobs\
            SET status = 'done', last_update = now(), execution_time = %s\
            WHERE id = %d AND status = 'processing'"
        cursor.execute(query, (execution_time, job_id))

        self._conn.commit()
