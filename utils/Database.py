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

        query = "SELECT id, strategy, asset, year, timeframe, params\
            FROM jobs\
            WHERE status='processing' AND worker=%s\
            LIMIT 1"
        cursor.execute(query, (worker_id,))

        result = cursor.fetchone()

        if result is None:
            return None

        params = {}
        if result[6] is not None and len(result[6]) > 0:
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
            WHERE status = 'idle'\
            LIMIT 1"
        cursor.execute(query, (worker_id,))

        self._conn.commit()

    def save_job_results(self, job_id, results):
        cursor = self._conn.cursor()

        query = "REPLACE INTO results\
            (job_id, session, orders, winning_ratio, net_profit, average_gain, average_loss, profit_factor)\
            VALUES (%s, %s, %s, %s, %s, %s, %s ,%s)"
        cursor.execute(query, (
            job_id, 'all',
            int(results['orders']),
            float(results['winning_ratio']),
            float(results['net_profit']),
            float(results['average_gain']),
            float(results['average_loss']),
            float(results['profit_factor'])
        ))

        self._conn.commit()

    def finish_job_by_id(self, job_id, execution_time):
        cursor = self._conn.cursor()

        query = "UPDATE jobs\
            SET status = 'done', last_update = now(), execution_time = %s\
            WHERE id = %s AND status = 'processing'"
        cursor.execute(query, (int(execution_time), job_id))

        self._conn.commit()
