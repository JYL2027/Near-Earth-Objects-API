import pytest
import json
from NEO_api import app 
import pandas as pd
import requests

BASE_URL = "http://127.0.0.1:5000"

def test_get_data_route():
    response = requests.get(f"{BASE_URL}/data")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)

def test_query_velocity_route():
    response = requests.get(f"{BASE_URL}/data/velocity_query", params={"min": 10, "max": 30})
    assert response.status_code == 200
    result = response.json()
    assert isinstance(result, dict)

def test_max_diam_route():
    response = requests.get(f"{BASE_URL}/data/max_diam/1.0")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict) or isinstance(data, list)

def test_biggest_neos_route():
    response = requests.get(f"{BASE_URL}/data/biggest_neos/5")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) <= 5

def test_now_neos_route():
    response = requests.get(f"{BASE_URL}/now/3")
    
    print(f"Response status code: {response.status_code}")
    print(f"Response body: {response.json()}")

    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, dict)  # Corrected from list to dict
    assert len(data) == 3 or len(data) < 3  # Accept fewer if not enough future entries


def test_delete_data_route():
    response = requests.post(f"{BASE_URL}/data")
    assert response.status_code == 200

    response = requests.delete(f"{BASE_URL}/data")
    assert response.status_code == 200
    assert response.text == 'Database flushed\n'

    response = requests.get(f"{BASE_URL}/data")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 0

def test_get_dates_route():
    response = requests.get(f"{BASE_URL}/data/date")
    assert response.status_code == 200
    dates = response.json()
    assert isinstance(dates, list)  # Expecting a list of dates

def test_get_data_by_year_route():
    year = '2025'
    response = requests.get(f"{BASE_URL}/data/{year}")
    assert response.status_code == 200
    data = response.json()
    for key in data:
        assert key.startswith(year)  # All data should be for the given year

def test_get_distances_route():
    response = requests.get(f"{BASE_URL}/data/distance", params={"min": 0.05, "max": 0.5})
    assert response.status_code == 200
    result = response.json()
    assert isinstance(result, dict)  # The result should be a dictionary

def test_query_velocity_route_with_valid_input():
    response = requests.get(f"{BASE_URL}/data/velocity_query", params={"min": 10, "max": 30})
    assert response.status_code == 200
    result = response.json()
    assert isinstance(result, dict)  # The result should be a dictionary

def test_query_diameter_route():
    response = requests.get(f"{BASE_URL}/data/max_diam/1.0")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict) or isinstance(data, list)  # Result can be either

def test_find_biggest_neos_route():
    response = requests.get(f"{BASE_URL}/data/biggest_neos/5")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) <= 5  # Ensuring no more than 5 results

def test_get_timeliest_neos_route():
    response = requests.get(f"{BASE_URL}/now/3")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)  # Should be a dictionary of NEOs

def test_create_job_route():
    job_data = {
        "start_date": "2025-Jan-01",
        "end_date": "2025-Jan-31",
        "kind": "1"
    }
    response = requests.post(f"{BASE_URL}/jobs", json=job_data)
    assert response.status_code == 200
    job = response.json()
    assert "id" in job  # Ensure that a job ID is returned

def test_list_jobs_route():
    response = requests.get(f"{BASE_URL}/jobs")
    assert response.status_code == 200
    job_ids = response.json()
    assert isinstance(job_ids, list)  # Should be a list of job IDs

def test_get_job_route():
    response = requests.post(f"{BASE_URL}/jobs", json={"start_date": "2025-Jan-01", "end_date": "2025-Jan-31", "kind": "1"})
    assert response.status_code == 200
    job_id = response.json()['id']

    response = requests.get(f"{BASE_URL}/jobs/{job_id}")
    assert response.status_code == 200

def test_get_job_results_route():
    job_id = "12345"
    response = requests.get(f"{BASE_URL}/results/{job_id}")
    print(response.content[:100])
    assert response.status_code == 200