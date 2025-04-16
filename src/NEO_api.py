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
from jobs import add_job, get_job_by_id, get_job_result
from flask import Flask, jsonify, request, Response
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

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

app = Flask(__name__)
rd = redis.Redis(host='localhost', port=6379, db=0)

@app.route('/data', methods=['POST'])
def fetch_neo_data():
    """

    Uses Selenium to download NEO data CSV from NASA CNEOS, parses it,
    and stores each entry in Redis using the close-approach date as the key.

    """
    try:
        download_dir = "/tmp/neo_csv_download"
        os.makedirs(download_dir, exist_ok=True)

        # Set up headless Chrome with download prefs
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "directory_upgrade": True,
        })

        driver = webdriver.Chrome(options=chrome_options)
        driver.get("https://cneos.jpl.nasa.gov/ca/")
        time.sleep(5)  

        # Find and click the CSV download button
        button = driver.find_element("xpath", '//button[contains(text(), "Download Table as CSV")]')
        button.click()
        time.sleep(5)  

        # Find the downloaded CSV file
        files = os.listdir(download_dir)
        csv_file = next((f for f in files if f.endswith(".csv")), None)
        if not csv_file:
            return "CSV file not found.\n"

        full_path = os.path.join(download_dir, csv_file)

        # Parse CSV and store in Redis
        neo_data = {"objects": []}
        with open(full_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                neo_data["objects"].append(dict(row))
                key = row["Close-Approach (CA) Date"]
                rd.set(key, json.dumps(row))

        driver.quit()
        return f"✅ Stored {len(neo_data['objects'])} NEO entries in Redis.\n"

    except Exception as e:
        logging.error(f"Error fetching NEO data: {e}")
        return f"Error fetching data: {e}\n"


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
