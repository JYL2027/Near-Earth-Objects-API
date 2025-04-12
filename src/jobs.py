import json
import uuid
import redis
import os
from hotqueue import HotQueue


REDIS_IP = os.environ.get("REDIS_IP", "redis-db")
# Initialize Redis client
rd = redis.Redis(host=REDIS_IP, port=6379, db=0)
q = HotQueue("queue", host=REDIS_IP, port=6379, db=1)
jdb = redis.Redis(host=REDIS_IP, port=6379, db=2)
rdb = redis.Redis(host=REDIS_IP, port=6379, db=3)

def _generate_jid():
    """
    Generate a pseudo-random identifier for a job.
    """
    return str(uuid.uuid4())

def _instantiate_job(jid, status, start, end):
    """
    Create the job object description as a python dictionary. Requires the job id,
    status, start and end parameters.
    """
    return {'id': jid,
            'status': status,
            'start': start,
            'end': end }

def _save_job(jid, job_dict):
    """Save a job object in the Redis database."""
    jdb.set(jid, json.dumps(job_dict))
    return

def _queue_job(jid):
    """Add a job to the redis queue."""
    q.put(jid)
    return

def add_job(start, end, status="submitted"):
    """Add a job to the redis queue."""
    jid = _generate_jid()
    job_dict = _instantiate_job(jid, status, start, end)
    _save_job(jid, job_dict)
    _queue_job(jid)
    return job_dict

def get_job_by_id(jid):
    """Return job dictionary given jid"""
    return json.loads(jdb.get(jid))

def update_job_status(jid, status):
    """Update the status of job with job id `jid` to status `status`."""
    job_dict = get_job_by_id(jid)
    if job_dict:
        job_dict['status'] = status
        _save_job(jid, job_dict)
    else:
        raise Exception()

def store_job_result(job_id: str, result):
    """Stores job result data into the results Redis database."""
    try:
        rdb.set(job_id, json.dumps(result))

    except Exception as e:
        print(f"Error storing result for job {job_id}: {e}")

def get_job_result(job_id: str):
    """Fetches the result of a job from the results Redis database."""
    try:
        result_data = rdb.get(job_id)

        if result_data is None:
            return None
        
        return json.loads(result_data)
    except Exception as e:
        print(f"Error fetching result for job {job_id}: {e}")
        return None