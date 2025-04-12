import json
import uuid
import redis
import time
import logging
import socket
import os
from tabulate import tabulate
import re
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

if __name__ == "__main__":
    logging.info("Worker started...")
    process_status()
