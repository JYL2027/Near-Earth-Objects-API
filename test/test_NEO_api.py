import pytest
import json
from NEO_api import app 
import pandas as pd

import pytest
import requests
import json

BASE_URL = "http://127.0.0.1:5000"

def test_epochs_route():
    response = requests.get(f"{BASE_URL}/epochs")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_specific_epoch_route():
    response = requests.get(f"{BASE_URL}/epochs")
    assert response.status_code == 200
    epoch_list = response.json()
    if not epoch_list:
        pytest.skip("No epochs returned")
    epoch_id = epoch_list[0]

    response = requests.get(f"{BASE_URL}/epochs/{epoch_id}")
    assert response.status_code == 200
    assert isinstance(response.json(), dict)

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
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
