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
from flask import Flask, jsonify, request, Response, send_file
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
    except FileNotFoundError:
        return 'NEO file not found'
    try:
        
        data['Minimum Diameter'] = data['Diameter'].apply(create_min_diam_column)
        data['Maximum Diameter'] = data['Diameter'].apply(create_max_diam_column)
        
        for idx, row in data.iterrows():
            dict_data = {'Object' : row['Object'], 'Close-Approach (CA) Date' : row['Close-Approach (CA) Date'], 'CA DistanceNominal (au)' : row['CA DistanceNominal (au)'], 'CA DistanceMinimum (au)' : row['CA DistanceMinimum (au)'], 'V relative(km/s)' : row['V relative(km/s)'], 'V infinity(km/s)':  row['V infinity(km/s)'], 'H(mag)' : row['H(mag)'], 'Diameter' : row['Diameter'],'Rarity' : row['Rarity'], 'Minimum Diameter' : row['Minimum Diameter'], 'Maximum Diameter' : row['Maximum Diameter']}
            
            original_date = row['Close-Approach (CA) Date']
            date_only = original_date.split()[0]
            key = f"{date_only}"

            rd.set(key, json.dumps(dict_data, sort_keys=True))
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
    if not year.isnumeric():
        return 'invalid year entered'
    
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
    
    if not (request.args.get('min').isnumeric() and request.args.get('max').isnumeric()):
        return 'invalid date range entered'
    
    min_velocity = float(request.args.get('min'))
    max_velocity = float(request.args.get('max'))

    if min_velocity > max_velocity:
        return 'min velocity must be less than max velocity'

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
    ''' 
    This function returns the n closest NEO's in time to right now.
        Args:
            count - the number of closest NEO's the user wants to return
        Returns:
            results (JSON) - a JSON dictionary contanining the n closest NEO's in time
    '''
    # convert count to int
    num_neo = int(count)
    # get current time
    current_time = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    logging.info(current_time)
    # intialize empty dict to hold full data
    dat = {}
    # retrive all data from redis
    for key in rd.keys('*'):
        key = key.decode('utf-8')
        dat[key] = json.loads(rd.get(key).decode('utf-8'))
    

    #logging.info(ordered_dict_dat)

    # initialize dict to hold cleaned keys (without the uncertainty part)
    cleaned_dict = {}

    # loop thru keys and clean them, saving them to new dict where values are its original values
    for i in dat.keys():
        clean_time = i.split("\\")[0].split('±')[0].rstrip()
        cleaned_dict[clean_time] = dat.get(i)

    # initialize list to hold all future timestamps
    future_keys_clean = []

    for i in cleaned_dict.keys():
        dt = datetime.strptime(i, "%Y-%b-%d %H:%M")
        # if date of timestamp is greater than current time, add it to list
        if current_time <= dt:
            future_keys_clean.append(i)

    # sort future keys based on timestamp
    sorted_keys = sorted(future_keys_clean, key=lambda x: datetime.strptime(x, "%Y-%b-%d %H:%M"))

    # initalize final results dict
    results = {}
    # find first n keys and return that
    for j in sorted_keys[:num_neo]:
        results[j] = cleaned_dict.get(j)
    
    return results

@app.route('/results/<job_id>', methods = ['GET'])
def get_results(job_id : str) -> Response:
    '''
    This function returns the output of the job given a specific ID
        Args:
            job_id (str) - a specific job ID
        Returns:
            output.png (image) - the plot that the job generates, saved to the working directory 
    '''
    
    if not rdb.get(f"{job_id}_output_plot"):
        return 'Job ID not found\n'
    
    if get_job_by_id(job_id)['status'] == 'complete': # check for completion
        try:
            with open("output.png", 'wb') as f: # open new file to store image bytes and write to it
                f.write(rdb.get(f"{job_id}_output_plot"))
        except:
            logging.error(f'Could not open new file to write bytes to')
            return "error"
        return send_file("output.png", mimetype='image/png', as_attachment=True) # send bytes for putput png to output stream
    else:
        return "Job still in progress"


# @app.route('/help', methods = ["GET"])
# def get_help():

#     help = "cURL routes:" \
#     " To view data: "
        

    



if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
