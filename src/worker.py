import json
import redis
import logging
import socket
import os
import matplotlib.pyplot as plt
from hotqueue import HotQueue
from jobs import update_job_status, store_job_result
from utils import clean_to_date_only, parse_date

REDIS_IP = os.environ.get("REDIS_IP", "redis-db")
rd = redis.Redis(host=REDIS_IP, port=6379, db=0)
q = HotQueue("queue", host=REDIS_IP, port=6379, db=1)
jdb = redis.Redis(host=REDIS_IP, port=6379, db=2)

# Results data base
rdb = redis.Redis(host=REDIS_IP, port=6379, db=3)

# Set logging
log_level_str = os.environ.get("LOG_LEVEL", "DEBUG").upper()
log_level = getattr(logging, log_level_str, logging.ERROR)

format_str=f'[%(asctime)s {socket.gethostname()}] %(filename)s:%(funcName)s:%(lineno)s - %(levelname)s: %(message)s'
logging.basicConfig(level=log_level, format=format_str)
logging.getLogger("matplotlib").setLevel(logging.WARNING)


@q.worker
def do_work(jobid: str) -> None:
    """
    This worker function to generate a relative velocity vs. distance, hexbin plot and stores the image in Redis
        Args:
            jobid (str) : The jobid as a string
        Returns:
            None
    """

    try:
        logging.info(f"Starting job {jobid}")
        update_job_status(jobid, "in progress")

    except Exception as e:
        logging.error(f"Error processing job {jobid}: {str(e)}")
        update_job_status(jobid, "failed")
    
    try:
        logging.debug("Retrieving start and end dates")
        job_raw = jdb.get(jobid)
        if not job_raw:
            raise ValueError("Job data not found in Redis")
        
        job_data = json.loads(job_raw)
        start_date_str = job_data.get('start')
        end_date_str = job_data.get('end')
        kind = job_data.get('kind')

        start_date = parse_date(start_date_str)
        end_date = parse_date(end_date_str)

        logging.info(start_date)
        logging.info(end_date)

    except Exception as e:
        raise ValueError(f"Invalid job data: {str(e)}")
        
    velocities = []
    distances = []
    mags = []
    raritys = []
    days = []
    processed_count = 0


    for key in rd.keys('*'):
        try:
            # logging.debug("Going through Redis and retrieving data...")
            key_str = key.decode('utf-8')
            neo_raw = rd.get(key)
            if not neo_raw:
                continue
        except Exception as e:
            logging.warning(f"Skipping key {key_str}: {str(e)}")
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
        # logging.debug(f"Checking date range of {neo_date}")
        if start_date <= neo_date <= end_date:
            try:
                velocity = float(neo.get("V relative(km/s)", 0))
                distance = float(neo.get("CA DistanceNominal (au)", 
                                neo.get("CA DistanceMinimum (au)", 0)))
                mag = float(neo.get('H(mag)', 0))
                rar = float(neo.get('Rarity', 0))
                velocities.append(velocity)
                distances.append(distance)
                mags.append(mag)
                raritys.append(rar)
                days.append(int(neo_date.day))

                processed_count += 1
            except (ValueError, TypeError) as e:
                logging.warning(f"Skipping {key_str}: invalid data {str(e)}")

    logging.info(f"Processed {processed_count} NEOs for job {jobid}")

    if not velocities or not distances:
        raise ValueError("No valid NEO data found in date range")
    

    if kind == '1':
        # Generate plot
        plt.figure(figsize=(12, 7))
        hb = plt.hexbin(distances, velocities, 
                gridsize=30,
                cmap='viridis',
                mincnt=1,
                edgecolors='none')
        plt.colorbar(hb, label='NEO Count')
        plt.title(f'NEO Close Approach Distance vs Relative Velocity: {start_date_str} to {end_date_str}')
        plt.xlabel('Close Approach Distance (AU)')
        plt.ylabel('Relative Velocity (km/s)')
        
        # Save plot to image and store in Redis
        logging.debug("Saving plot to Redis")
        plt.savefig(f'/app/{jobid}_plot.png')

    elif kind == '2':
        min_mag = min(mags)
        max_mag = max(mags)
        
        norm_mags = [(mag - min_mag) / (max_mag - min_mag) * 100 + 1 for mag in mags]
        plt.figure(figsize=(12,7))
        scatter = plt.scatter(days, velocities, s= norm_mags, c = raritys)
        plt.legend(*scatter.legend_elements(), title = "Rarity")
        plt.ylim(0,30)
        plt.xlim(0,31)
        plt.xlabel('Day of Month')
        plt.ylabel('V relative(km/s)')
        plt.title(f"NEO's Approaching {start_date.month} {start_date.year}")
        plt.savefig(f'/app/{jobid}_plot.png')

    else:
        logging.error('Value for kind is invalid')

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


if __name__ == "__main__":
    logging.info("Worker started...")
    do_work()
