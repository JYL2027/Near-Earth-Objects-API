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


def clean_to_date_only(time: str) -> str:
    ''' 
    Cleans a NEO time string and extracts only the date part.

    Args:
        time (str): The raw time string.

    Returns:
        str: The date (YYYY-MMM-DD).
    '''
    if not time:
        return ""

    # First, remove anything after ± if it exists
    if '±' in time:
        time = time.split('±')[0].strip()

    # Then, split off the time part and keep only the date
    parts = time.split()
    if parts:
        return parts[0]  # Return only the date part
    else:
        return time.strip() 


def parse_date(date_str: str) -> datetime:
    '''
    This function parces the date given to be a datetime object

    Args:
        date_str: The date to be parsed as a string
    
    Returns:
        Returns the date format as a datetime object
    '''
    try:
        # Directly parse 
        return datetime.strptime(date_str.strip(), "%Y-%b-%d")
    except ValueError:
        raise ValueError(f"Unrecognized date format: {date_str}")
    
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
        logging.info(f"Starting job {jobid}")
        update_job_status(jobid, "in progress")
    
        try:
            job_raw = jdb.get(jobid)
            if not job_raw:
                raise ValueError("Job data not found in Redis")
            
            job_data = json.loads(job_raw)
            start_date_str = job_data.get('start')
            end_date_str = job_data.get('end')

            start_date = parse_date(start_date_str)
            end_date = parse_date(end_date_str)

        except Exception as e:
            raise ValueError(f"Invalid job data: {str(e)}")
        
        velocities = []
        distances = []
        processed_count = 0

        for key in rd.keys('*'):
            try:
                key_str = key.decode('utf-8')
                neo_raw = rd.get(key)
                if not neo_raw:
                    continue

                neo = json.loads(neo_raw.decode('utf-8'))
                neo_date_str = neo.get('Close-Approach (CA) Date', '')
                
                if not neo_date_str:
                    continue

                # Parse NEO date
                try:
                    neo_date = clean_to_date_only(neo_date_str)
                    neo_date = parse_date(neo_date)
                except ValueError as e:
                    logging.warning(f"Skipping {key_str}: {str(e)}")
                    continue

                # Check date range
                if start_date <= neo_date <= end_date:
                    try:
                        velocity = float(neo.get("V relative(km/s)", 0))
                        distance = float(neo.get("CA DistanceNominal (au)", 
                                      neo.get("CA DistanceMinimum (au)", 0)))
                        velocities.append(velocity)
                        distances.append(distance)
                        processed_count += 1
                    except (ValueError, TypeError) as e:
                        logging.warning(f"Skipping {key_str}: invalid data - {str(e)}")

            except Exception as e:
                logging.warning(f"Skipping key {key_str}: {str(e)}")
                continue

        logging.info(f"Processed {processed_count} NEOs for job {jobid}")

      
        if not velocities or not distances:
            raise ValueError("No valid NEO data found in date range")
        # Generate scatter plot
        plt.figure(figsize=(12, 7))
        hb = plt.hexbin(distances, velocities, 
               gridsize=30,
               cmap='viridis',
               mincnt=1,
               edgecolors='none')
        plt.colorbar(hb, label='NEO Count')
        plt.title(f'NEO Density: {start_date_str} to {end_date_str}')
        plt.xlabel('Close Approach Distance (AU)')
        plt.ylabel('Relative Velocity (km/s)')

        
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
