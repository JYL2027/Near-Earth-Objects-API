import json
import uuid
import redis
import time
import logging
import socket
import os
from tabulate import tabulate
import re
import matplotlib.pyplot as plt
from io import BytesIO
from datetime import datetime
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
logging.basicConfig(level= 'DEBUG', format=format_str)


@q.worker
def do_work(jobid):
    """
    This worker function to generate a relative velocity vs. distance scatter plot and stores the image in Redis

    Args:
        The jobid as a string

    Return:
        None
    """

    try:
        print(f"Starting job {jobid}")
        update_job_status(jobid, "in progress")

        job_data = json.loads(jdb.get(jobid))
        start_date_str = job_data.get('start_date')
        end_date_str = job_data.get('end_date')
        
        try:
            start_date = datetime.strptime(start_date_str, "%Y-%b-%d %H:%M")
        except ValueError:
                start_date = datetime.strptime(start_date_str, "%Y-%b-%d")
                
        try:
            end_date = datetime.strptime(end_date_str, "%Y-%b-%d %H:%M")
        except ValueError:
            end_date = datetime.strptime(end_date_str, "%Y-%b-%d")

        velocities = []
        distances = []

        for key in rd.keys('*'):
            key_str = key.decode('utf-8')

            try:
                clean_date = key_str.split("\\")[0].split('±')[0].strip()
                date_obj = datetime.strptime(clean_date, "%Y-%b-%d %H:%M")
            except Exception as e:
                logging.error(f"Skipping key {key_str}: invalid datetime format")
                continue

            if start_date <= date_obj <= end_date:
                try:
                    neo = json.loads(rd.get(key).decode('utf-8'))
                    velocity = float(neo.get("V relative(km/s)", 0))
                    distance = float(neo.get("CA DistanceNominal (au)") or neo.get("CA DistanceMinimum (au)", 0))

                    velocities.append(velocity)
                    distances.append(distance)
                except (ValueError, TypeError, json.JSONDecodeError) as e:
                    logging.warning(f"Skipping key {key_str} due to error: {e}")
                    continue

        # Generate scatter plot
        plt.figure(figsize=(10, 6))
        plt.scatter(distances, velocities, alpha=0.7, edgecolors='k')
        plt.title('Relative Velocity vs. Close-Approach Distance')
        plt.xlabel('Distance (AU)')
        plt.ylabel('Relative Velocity (km/s)')
        plt.grid(True)

        # Save plot to image buffer and store in Redis
        plt.savefig(f'/app/{jobid}_plot.png')
        update_job_status(jobid, "complete")
        logging.info(f"Job {jobid} complete.")
        try:
            file_bytes = open(f'/app/{jobid}_plot.png', 'rb').read() # read in image as bytes
            logging.info('read plot in..')
        except:
            if not file_bytes:
                logging.error('error producing output file')
            else:
                logging.error('error reading output file')
        # set the file bytes as a key in Redis
        try:
            # set key value pair to odb where key is name of plot and value is its data in bytes
            rdb.set(f"{jobid}_output_plot", file_bytes) 
            logging.info('saved output file to odb')
        except:
            logging.error('error pushing output file to Redis')

    except Exception as e:
        logging.error(f"Error processing job {jobid}: {str(e)}")
        update_job_status(jobid, "failed")

if __name__ == "__main__":
    logging.info("Worker started...")
    do_work()
