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

    def get_next_processing_job_by_worker(self, worker_id, version):
        cursor = self._conn.cursor()

        query = "SELECT id, strategy, asset, year, timeframe, params, cost, pip_value\
            FROM jobs\
            INNER JOIN trading_cost USING (asset)\
            WHERE status='processing' AND worker=%s AND version=%s\
            LIMIT 1"
        cursor.execute(query, (worker_id, version))

        result = cursor.fetchone()

        if result is None:
            return None

        params = {}
        if result[5] is not None and len(result[5]) > 0:
            params = json.loads(result[5])
        params['asset'] = result[2]
        params['year'] = result[3]
        params['timeframe'] = result[4]
        params['trading_cost'] = result[6]
        params['pip_value'] = result[7]

        return {
            "id": result[0],
            "strategy": result[1],
            "params": params
        }

    def assign_job_to_worker(self, worker_id, version):
        cursor = self._conn.cursor()

        query = "UPDATE jobs\
            SET status = 'processing', worker = %s, last_update = now()\
            WHERE status = 'idle' AND version=%s\
            LIMIT 1"
        cursor.execute(query, (worker_id, version))

        self._conn.commit()

    def save_job_results(self, job_id, results):
        cursor = self._conn.cursor()

        query = "REPLACE INTO results\
            (job_id, session, orders, winning_ratio, net_profit, average_gain, average_loss, profit_factor)\
            VALUES (%s, %s, %s, %s, %s, %s, %s ,%s)"

        for sess, res in results.items():
            cursor.execute(query, (
                job_id, sess,
                int(res['orders']),
                float(res['winning_ratio']),
                float(res['net_profit']),
                float(res['average_gain']),
                float(res['average_loss']),
                float(res['profit_factor'])
            ))

        self._conn.commit()

    def finish_job_by_id(self, job_id, execution_time):
        cursor = self._conn.cursor()

        query = "UPDATE jobs\
            SET status = 'done', last_update = now(), execution_time = %s\
            WHERE id = %s AND status = 'processing'"
        cursor.execute(query, (int(execution_time), job_id))

        self._conn.commit()
