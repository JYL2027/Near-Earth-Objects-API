import pytest
import json
from NEO_api import app 
import pandas as pd
import requests

BASE_URL = "http://127.0.0.1:5000"

def safe_json(response):
    try:
        return response.json()
    except Exception as e:
        print("Failed to parse JSON. Status:", response.status_code)
        print("Raw content:", response.text)
        raise e
    
def test_get_data_route():
    response = requests.get(f"{BASE_URL}/data")
    assert response.status_code == 200
    data = safe_json(response)
    assert isinstance(data, dict)

def test_query_velocity_route():
    response = requests.get(f"{BASE_URL}/data/velocity_query", params={"min": 10, "max": 30})
    assert response.status_code == 200
    result = safe_json(response)
    assert isinstance(result, dict)

def test_max_diam_route():
    response = requests.get(f"{BASE_URL}/data/max_diam/1.0")
    assert response.status_code == 200
    data = safe_json(response)
    assert isinstance(data, (dict, list))

def test_biggest_neos_route():
    response = requests.get(f"{BASE_URL}/data/biggest_neos/5")
    assert response.status_code == 200
    data = safe_json(response)
    assert isinstance(data, list)
    assert len(data) <= 5

def test_now_neos_route():
    response = requests.get(f"{BASE_URL}/now/3")
    assert response.status_code == 200
    data = safe_json(response)
    assert isinstance(data, dict)
    assert len(data) <= 3

def test_delete_data_route():
    response = requests.post(f"{BASE_URL}/data")
    assert response.status_code == 200

    response = requests.delete(f"{BASE_URL}/data")
    assert response.status_code == 200
    assert response.text.strip() == 'Database flushed'

    response = requests.get(f"{BASE_URL}/data")
    assert response.status_code == 200
    data = safe_json(response)
    assert len(data) == 0

def test_get_dates_route():
    response = requests.get(f"{BASE_URL}/data/date")
    assert response.status_code == 200
    dates = safe_json(response)
    assert isinstance(dates, list)

def test_get_data_by_year_route():
    year = '2025'
    response = requests.get(f"{BASE_URL}/data/{year}")
    assert response.status_code == 200
    data = safe_json(response)
    for key in data:
        assert key.startswith(year)

def test_get_distances_route():
    response = requests.get(f"{BASE_URL}/data/distance", params={"min": 0.05, "max": 0.5})
    assert response.status_code == 200
    result = safe_json(response)
    assert isinstance(result, list)
    for obj in result:
        dist = float(obj.get('Distance (AU)', 0))
        assert 0.05 <= dist <= 0.5

def test_query_velocity_route_with_valid_input():
    response = requests.get(f"{BASE_URL}/data/velocity_query", params={"min": 10, "max": 30})
    assert response.status_code == 200
    result = safe_json(response)
    assert isinstance(result, dict)

def test_query_diameter_route():
    response = requests.get(f"{BASE_URL}/data/max_diam/1.0")
    assert response.status_code == 200
    data = safe_json(response)
    assert isinstance(data, (dict, list))

def test_find_biggest_neos_route():
    response = requests.get(f"{BASE_URL}/data/biggest_neos/5")
    assert response.status_code == 200
    data = safe_json(response)
    assert isinstance(data, list)
    assert len(data) <= 5

def test_get_timeliest_neos_route():
    response = requests.get(f"{BASE_URL}/now/3")
    assert response.status_code == 200
    data = safe_json(response)
    assert isinstance(data, dict)

def test_create_job_route():
    job_data = {
        "start_date": "2025-Jan-01",
        "end_date": "2025-Jan-31",
        "kind": "1"
    }
    response = requests.post(f"{BASE_URL}/jobs", json=job_data)
    assert response.status_code == 200
    job = safe_json(response)
    assert "id" in job

def test_list_jobs_route():
    response = requests.get(f"{BASE_URL}/jobs")
    assert response.status_code == 200
    job_ids = safe_json(response)
    assert isinstance(job_ids, list)

def test_get_job_route():
    response = requests.post(f"{BASE_URL}/jobs", json={
        "start_date": "2025-Jan-01",
        "end_date": "2025-Jan-31",
        "kind": "1"
    })
    assert response.status_code == 200
    job_id = safe_json(response)['id']

    response = requests.get(f"{BASE_URL}/jobs/{job_id}")
    assert response.status_code == 200

def test_get_job_results_route():
    # You need a valid job_id from an actual job before testing this.
    response = requests.post(f"{BASE_URL}/jobs", json={
        "start_date": "2025-Jan-01",
        "end_date": "2025-Jan-31",
        "kind": "1"
    })
    assert response.status_code == 200
    job_id = safe_json(response)["id"]

    response = requests.get(f"{BASE_URL}/results/{job_id}")
    assert response.status_code == 200
    data = safe_json(response)
    assert isinstance(data, (dict, list))