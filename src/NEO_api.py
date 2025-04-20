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

@app.route('/data', methods = ['POST'])
def fetch_neo_data():
    """
    This function downloads the data as a csv and uploads it to Redis.
        Args:
            None
        Returns:
            message (str): message indicating whether data push was successful
    """
    try:
        data = pd.read_csv('/app/neo.csv')
        for idx, row in data.iterrows():
            dict_data = {'Object' : row['Object'],'CA DistanceNominal (au)' : row['CA DistanceNominal (au)'], 'CA DistanceMinimum (au)' : row['CA DistanceMinimum (au)'], 'V relative(km/s)' : row['V relative(km/s)'], 'V infinity(km/s)':  row['V infinity(km/s)'], 'H(mag)' : row['H(mag)'], 'Diameter' : row['Diameter'],'Rarity' : row['Rarity']}
            rd.set(row['Close-Approach (CA) Date'], json.dumps(dict_data, sort_keys=True))
        if len(rd.keys('*')) == len(data):
            return 'success loading data\n'
        else:
            return 'failed to load all data into redis'
    except Exception as e:
        logging.error(f"Error downloading NEO data: {e}")
        return f"Error fetching data: {e}\n"

@app.route('/data', methods = ['GET'])
def return_neo_data():
    dat = {}
    for key in rd.keys('*'):
        key = key.decode('utf-8')
        try:
            val = json.loads(rd.get(key).decode('utf-8'))
            dat[key] = val
        except:
            logging.error(f'Error retrieving data at {key}')
        
    return json.dumps(dat)

@app.route('/data', methods = ["DELETE"])
def delete_neo_data():
    rd.flushdb()
    if not rd.keys():
        return 'Database flushed\n'
    else:
        return "Database failed to clear\n"
    
@app.route('/data/date', methods = ['GET'])
def get_year() -> list:
    '''
    This function returns all of the years and time values.
    Args:
        None
    Returns: A flask response containing the years/time as a list
    '''
    years = []
    for key in rd.keys('*'):
        key = key.decode('utf-8')
        years.append(key)
    return years

@app.route('/data/<year>', methods = ['GET'])
def get_data_by_year(year):
    '''
    This function returns the data for NEO's that will approach Earth in a given year.
        Args:
            year (str): the year for which you want NEO data for
        Returns:
            dat (dict) - subset of the data
    '''
    dat = {}
    for key in rd.keys('*'):
        key = key.decode('utf-8')
        if key.split('-')[0] == year:
            dat[key] = json.loads(rd.get(key).decode('utf-8'))
    return dat

@app.route('/data/distance', methods=['GET'])
def get_distances() -> Response:
    """
    Get all close-approach distances with optional filtering
    
    Query Parameters:
        min (float): Minimum distance in AU
        max (float): Maximum distance in AU
        
    Returns:
        JSON response with number of results and list of NEOs with their approach dates and distances
    """
    try:
        # Parse optional query parameters
        min_dist = request.args.get('min', type=float)
        max_dist = request.args.get('max', type=float)
        
        results = []
        
        for key in rd.keys('*'):
            key = key.decode('utf-8')
            neo = json.loads(rd.get(key).decode('utf-8'))
            
            try:
                # Get distance
                distance = float(neo.get('CA DistanceNominal (au)') or neo.get('CA DistanceMinimum (au)', 0))
            except (ValueError, TypeError):
                continue
                
            # Apply filters
            if min_dist is not None and distance < min_dist:
                continue
            if max_dist is not None and distance > max_dist:
                continue
                
            results.append({
                'date': key,
                'object': neo.get('Object', 'Unknown'),
                'distance_au': distance,
            })
        
        return jsonify({
            'count': len(results),
            'results': results
        })
        
    except Exception as e:
        logging.error(f"Error in get_distances: {str(e)}")
        return jsonify("Error in getting distance")
    
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
