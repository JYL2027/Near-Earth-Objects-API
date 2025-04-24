#!/usr/bin/env python3
import json
import logging
import requests
import redis
import socket
import uuid
import os
import csv
import numpy as np
import sys
import io
import time
import pprint
from datetime import datetime, timezone
from hotqueue import HotQueue
import pandas as pd
from jobs import add_job, get_job_by_id, get_job_result
from flask import Flask, jsonify, request, Response
from pre_work import create_min_diam_column, create_max_diam_column

# set logging
# format_str=f'[%(asctime)s {socket.gethostname()}] %(filename)s:%(funcName)s:%(lineno)s - %(levelname)s: %(message)s'
logging.basicConfig(level='DEBUG')


REDIS_IP = os.environ.get("REDIS_HOST", "redis-db")

# Initialize Redis client
rd = redis.Redis(host=REDIS_IP, port=6379, db=0)
q = HotQueue("queue", host=REDIS_IP, port=6379, db=1)
jdb = redis.Redis(host=REDIS_IP, port=6379, db=2)
rdb = redis.Redis(host=REDIS_IP, port=6379, db=3)

# Initialize app
app = Flask(__name__)

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
        
        data['Minimum Diameter'] = data['Diameter'].apply(create_min_diam_column)
        data['Maximum Diameter'] = data['Diameter'].apply(create_max_diam_column)
        
        for idx, row in data.iterrows():
            dict_data = {'Object' : row['Object'],'CA DistanceNominal (au)' : row['CA DistanceNominal (au)'], 'CA DistanceMinimum (au)' : row['CA DistanceMinimum (au)'], 'V relative(km/s)' : row['V relative(km/s)'], 'V infinity(km/s)':  row['V infinity(km/s)'], 'H(mag)' : row['H(mag)'], 'Diameter' : row['Diameter'],'Rarity' : row['Rarity'], 'Minimum Diameter' : row['Minimum Diameter'], 'Maximum Diameter' : row['Maximum Diameter']}
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
        
    return json.dumps(dat, ensure_ascii=False, sort_keys=True)

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
    
@app.route('/data/velocity_query', methods= ['GET'])
def query_velocity():
    min_velocity = float(request.args.get('min'))
    max_velocity = float(request.args.get('max'))

    dat = {}
    
    for key in rd.keys('*'):
        key = key.decode('utf-8')
        neo = json.loads(rd.get(key).decode('utf-8'))

        if min_velocity <= float(neo.get('V relative(km/s)')) <= max_velocity:
            dat[key] = json.loads(rd.get(key).decode('utf-8'))

    return dat


@app.route('/jobs', methods=['POST'])
def create_job() -> Response:
    """
    This function is a API route that creates a new job

    Args:
        None
    
    Returns:
        The function returns a Flask json reponse of the created job or if it failed to create the job
    """

    logging.debug("Creating job...")
    if not request.json:
        return jsonify("Error, invalid input for job")
    
    # Data packet must be json
    params = request.get_json()
    
    start_date = params.get("start_date")
    end_date = params.get("end_date")

    if start_date is None or end_date is None:
        return jsonify("Error missing start_date or end_date parameters")

    # Check if ID's are valid
    keys = rd.keys()
    ID = []
    logging.info("Filtering out Dates... ")
    for key in keys:
        # Decode the Key
        ID.append(key.decode('utf-8'))

    if start_date not in ID or end_date not in ID:
            return jsonify("Error: invalid HGNC ID's or no Data in Redis")
    
    # Add a job
    job = add_job(start_date, end_date)

    logging.debug(f"Job created and queued successfully.")
    return jsonify(job)

@app.route('/jobs', methods=['GET'])
def list_jobs() -> Response:
    """
    This function is a API route that lists all the job IDs

    Args:
        None

    Returns:
        The function returns all of the existing job ID's as a Flask json response
    """
    logging.debug("Listing job ID's...")

    job_ids = []
    job_keys = jdb.keys()

    if not job_keys:
        logging.warning("No IDs found in Redis")
        return jsonify("No job ID's currently")
    
    for key in job_keys:
        job_ids.append(key.decode('utf-8'))
    
    logging.debug("All job ID's found successfully")
    return jsonify(job_ids)

@app.route('/jobs/<jobid>', methods=['GET'])
def get_job(jobid: str) -> Response:
    """
    This function is a API route that retrieves job details by ID

    Args:
        jobid is the ID of the job you want to get information about as a string

    Returns:
        The function returns all of the job information for the given job ID as a Flask json response
    """
    logging.debug("Retrieving job details...")

    job = get_job_by_id(jobid)

    if not job:
        return jsonify("Error job not found")
    
    return jsonify(job)

@app.route('/results/<jobid>', methods = ['GET'])
def check_result(jobid : str) -> Response:
    """
    This function given a jobid will retrieve the job results or to see if the job is still in progress.

    Args:

        jobid is the ID of the job you want to get information about as a string

    Returns:
        Returns the results of a job as a Flask response
    """

    job = get_job_by_id(jobid)
    result = get_job_result(jobid)
    logging.debug("Retrieved job and result")
    
    if not job:
        return jsonify("Error job not found")
    
    if job["status"] != "complete":
        return jsonify(f"Job {jobid} is still in progress.")

    if not result:
        return jsonify("No result found")

    return Response(result)

@app.route('/data/max_diam/<max_diameter>', methods=['GET'])
def query_diameter(max_diameter):
    """
        This function is for an API endpoint. Given a max diameter, this route will find all
        the NEOs that are less than the input.

        Args:
            max_diameter: type - float/int. Upper bound for diameter

        Returns:
            All the NEOs less than the max_diameter. Compares the input
            to the max diameter of each NEO since it is a range.
    """
    max_diameter = float(max_diameter)
    results = {}

    for key in rd.keys('*'):
        key_str = key.decode('utf-8')
        neo = json.loads(rd.get(key).decode('utf-8'))

        diam_str = neo.get('Maximum Diameter')
        if diam_str:
            try:
                diam = float(diam_str)
                if diam <= max_diameter:
                    results[key_str] = neo
            except (ValueError, TypeError):
                continue  # skip if diameter isn't parseable

    return jsonify(results)

@app.route('/data/biggest_neos/<count>', methods=['GET'])
def find_biggest_neo(count):
    """
        This function is for an API endpoint. Given input, it will 
        find the x biggest NEOs based on the H scale.

        Args:
            count: type - int. How many NEOs you want returned

        Returns:
            List of dictionaries of count number of NEOs 
    """
    num_neo = int(count)
    dat = []
    for key in rd.keys('*'):
        key_str = key.decode('utf-8')
        value = json.loads(rd.get(key).decode('utf-8'))
        dat.append({key_str: value})
    
    def get_score(d):
        time_key = next(iter(d))
        return d[time_key].get("H(mag)", float('inf'))  # default if score missing
    
    sorted_data = sorted(dat, key=get_score)
    limit_data = sorted_data[:num_neo]
    return jsonify(limit_data)

@app.route('/now/<count>', methods = ['GET'])
def get_timeliest_neos(count):
    num_neo = int(count)
    current_time = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    logging.info(current_time)
    dat = {}

    for key in rd.keys('*'):
        key = key.decode('utf-8')
        dat[key] = json.loads(rd.get(key).decode('utf-8'))
    
    ordered_json_dat = json.dumps(dat, sort_keys=True)
    ordered_dict_dat = json.loads(ordered_json_dat)

    #logging.info(ordered_dict_dat)

    cleaned_dict = {}

    for i in ordered_dict_dat.keys():
        clean_time = i.split("\\")[0].split('±')[0].rstrip()
        #print(i)
        cleaned_dict[clean_time] = ordered_dict_dat.get(i)

    closest_time = datetime.strptime(list(cleaned_dict.keys())[-1], "%Y-%b-%d %H:%M") # use first epoch in Redis as sample time
    
    logging.info(closest_time)


    for i in cleaned_dict.keys():
        dt = datetime.strptime(i, "%Y-%b-%d %H:%M")
        if current_time <= dt <= closest_time:
            closest_time = dt
            key_to_use = i
    
    return json.dumps(cleaned_dict.get(key_to_use))



    



if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
