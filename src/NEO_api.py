#!/usr/bin/env python3
import json
import logging
import requests
import redis
import socket
import uuid
import os
from tabulate import tabulate
from hotqueue import HotQueue
from jobs import add_job, get_job_by_id, get_job_result
from flask import Flask, jsonify, request, Response

# Initialize app
app = Flask(__name__)

# Set logging
format_str=f'[%(asctime)s {socket.gethostname()}] %(filename)s:%(funcName)s:%(lineno)s - %(levelname)s: %(message)s'
logging.basicConfig(level=logging.ERROR, format = format_str)

REDIS_IP = os.environ.get("REDIS_HOST", "redis-db")

# Initialize Redis client
rd = redis.Redis(host=REDIS_IP, port=6379, db=0)
q = HotQueue("queue", host=REDIS_IP, port=6379, db=1)
jdb = redis.Redis(host=REDIS_IP, port=6379, db=2)
rdb = redis.Redis(host=REDIS_IP, port=6379, db=3)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
