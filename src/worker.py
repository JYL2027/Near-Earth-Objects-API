import json
import uuid
import redis
import time
import logging
import socket
import os
from tabulate import tabulate
import re
import matplotlib as plt
from io import BytesIO
from hotqueue import HotQueue
from jobs import update_job_status, store_job_result

REDIS_IP = os.environ.get("REDIS_IP", "redis-db")
rd = redis.Redis(host=REDIS_IP, port=6379, db=0)
q = HotQueue("queue", host=REDIS_IP, port=6379, db=1)
jdb = redis.Redis(host=REDIS_IP, port=6379, db=2)

# Results data base
rdb = redis.Redis(host=REDIS_IP, port=6379, db=3)

# Set logging
format_str=f'[%(asctime)s {socket.gethostname()}] %(filename)s:%(funcName)s:%(lineno)s - %(levelname)s: %(message)s'
logging.basicConfig(level=logging.ERROR, format=format_str)


@q.worker
def do_work(jobid):
    """
    Worker function to generate a relative velocity vs. distance scatter plot.
    Stores the image as a byte array in Redis result DB.
    """
    try:
        print(f"Starting job {jobid}")
        update_job_status(jobid, "in progress")

        job_data = json.loads(jdb.get(jobid))
        start_date = job_data['start_date']
        end_date = job_data['end_date']

        # Collect relevant NEO data between start and end dates
        velocities = []
        distances = []

        for key in rd.keys('*'):
            key_str = key.decode('utf-8')
            if start_date <= key_str <= end_date:
                neo = json.loads(rd.get(key).decode('utf-8'))

                try:
                    velocity = float(neo.get("V relative(km/s)", 0))
                    distance = float(neo.get("CA DistanceNominal (au)") or neo.get("CA DistanceMinimum (au)", 0))
                    velocities.append(velocity)
                    distances.append(distance)
                except (ValueError, TypeError):
                    continue

        # Generate plot
        plt.figure(figsize=(10, 6))
        plt.scatter(distances, velocities, alpha=0.7, edgecolors='k')
        plt.title('Relative Velocity vs. Close-Approach Distance')
        plt.xlabel('Distance (AU)')
        plt.ylabel('Relative Velocity (km/s)')
        plt.grid(True)

        # Save to Redis
        img_bytes = BytesIO()
        plt.savefig(img_bytes, format='png')
        img_bytes.seek(0)
        rdb.set(jobid, img_bytes.read())
        plt.close()

        update_job_status(jobid, "complete")
        print(f"Job {jobid} complete.")

    except Exception as e:
        print(f"Error processing job {jobid}: {str(e)}")
        update_job_status(jobid, "failed")

if __name__ == "__main__":
    logging.info("Worker started...")
    do_work()