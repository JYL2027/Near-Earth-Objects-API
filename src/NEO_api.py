#!/usr/bin/env python3
import json
import logging
import requests
import redis
import socket
import uuid
import os
import csv
import sys
import io
import time
from hotqueue import HotQueue
import pandas as pd
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

@app.route('/data', methods=['POST'])
def fetch_neo_data():
    """

    """
    try:



        data = pd.read_csv('NEO Earth Close Approaches.csv')
        for idx, row in data.iterrows():
            rd.hset(row['Close-Approach (CA) Date'], mapping={'Object': row['Object'],'CA DistanceNominal (au)': row['CA DistanceNominal (au)'], 'CA DistanceMinimum (au)':row['CA DistanceMinimum (au)'], 'V relative(km/s)':row['V relative(km/s)'], 'V infinity(km/s)':row['V infinity(km/s)'], 'H(mag)': row['H(mag)'], 'Diameter':row['Diameter'],'Rarity': row['Rarity'] })
        
        # # Parse CSV and store in Redis
        # neo_data = {"objects": []}
        # with open('NEO Earth Close Approaches.csv', newline='', encoding='utf-8') as f:
        #     reader = csv.DictReader(f)
        #     for row in reader:
        #         neo_data["objects"].append(dict(row))
        #         key = row["Close-Approach (CA) Date"]
        #         rd.set(key, json.dumps(row))

        # return f"Stored {len(neo_data['objects'])} NEO entries in Redis.\n"

    except Exception as e:
        logging.error(f"Error fetching NEO data: {e}")
        return f"Error fetching data: {e}\n"

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
