import pytest
import json
from NEO_api import app 
import pandas as pd

@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

# -------------------------------
# /data [POST]
# -------------------------------
def test_fetch_neo_data_success(client, mocker):
    # Mock reading CSV and Redis set
    mocker.patch('pandas.read_csv', return_value=pd.DataFrame({
        'Object': ['TestObject'],
        'Close-Approach (CA) Date': ['2025-Jan-01 12:00'],
        'CA DistanceNominal (au)': [0.5],
        'CA DistanceMinimum (au)': [0.4],
        'V relative(km/s)': [15.5],
        'V infinity(km/s)': [14.8],
        'H(mag)': [22.1],
        'Diameter': [0.1],
        'Rarity': ['common']
    }))
    mocker.patch('your_api_file_name.rd.set')
    mocker.patch('your_api_file_name.rd.keys', return_value=[b'2025-Jan-01 12:00'])

    response = client.post('/data')
    assert response.status_code == 200
    assert b'success' in response.data

def test_fetch_neo_data_missing_file(client, mocker):
    mocker.patch('pandas.read_csv', side_effect=FileNotFoundError)
    response = client.post('/data')
    assert response.status_code == 200
    assert b'NEO file not found' in response.data

# -------------------------------
# /data [GET]
# -------------------------------
def test_return_neo_data_success(client, mocker):
    mocker.patch('your_api_file_name.rd.keys', return_value=[b'2025-Jan-01 12:00'])
    mocker.patch('your_api_file_name.rd.get', return_value=json.dumps({'Object': 'TestObject'}).encode('utf-8'))
    
    response = client.get('/data')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert '2025-Jan-01 12:00' in data

# -------------------------------
# /data [DELETE]
# -------------------------------
def test_delete_neo_data_success(client, mocker):
    mocker.patch('your_api_file_name.rd.flushdb')
    mocker.patch('your_api_file_name.rd.keys', return_value=[])

    response = client.delete('/data')
    assert response.status_code == 200
    assert b'Database flushed' in response.data

# -------------------------------
# /data/date [GET]
# -------------------------------
def test_get_date_success(client, mocker):
    mocker.patch('your_api_file_name.rd.keys', return_value=[b'2025-Jan-01 12:00'])

    response = client.get('/data/date')
    assert response.status_code == 200
    dates = json.loads(response.data)
    assert '2025-Jan-01 12:00' in dates

# -------------------------------
# /data/<year> [GET]
# -------------------------------
def test_get_data_by_year_success(client, mocker):
    mocker.patch('your_api_file_name.rd.keys', return_value=[b'2025-Jan-01 12:00'])
    mocker.patch('your_api_file_name.rd.get', return_value=json.dumps({'Object': 'TestObject'}).encode('utf-8'))

    response = client.get('/data/2025')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert '2025-Jan-01 12:00' in data

def test_get_data_by_year_invalid(client):
    response = client.get('/data/abcd')
    assert response.status_code == 200
    assert b'invalid year entered' in response.data

# -------------------------------
# /data/distance [GET]
# -------------------------------
def test_get_distances_success(client, mocker):
    mocker.patch('your_api_file_name.rd.keys', return_value=[b'2025-Jan-01 12:00'])
    mocker.patch('your_api_file_name.rd.get', return_value=json.dumps({'CA DistanceNominal (au)': 0.5}).encode('utf-8'))

    response = client.get('/data/distance?min=0.1&max=1.0')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['count'] == 1

# -------------------------------
# /data/velocity_query [GET]
# -------------------------------
def test_query_velocity_success(client, mocker):
    mocker.patch('your_api_file_name.rd.keys', return_value=[b'2025-Jan-01 12:00'])
    mocker.patch('your_api_file_name.rd.get', return_value=json.dumps({'V relative(km/s)': 15.0}).encode('utf-8'))

    response = client.get('/data/velocity_query?min=10&max=20')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert '2025-Jan-01 12:00' in data

def test_query_velocity_invalid(client):
    response = client.get('/data/velocity_query?min=abc&max=xyz')
    assert response.status_code == 200
    assert b'invalid date range entered' in response.data

# -------------------------------
# /jobs [POST]
# -------------------------------
def test_create_job_success(client, mocker):
    mocker.patch('your_api_file_name.rd.keys', return_value=[b'2025-Jan-01 12:00'])
    mocker.patch('your_api_file_name.add_job', return_value={'job_id': '123', 'status': 'submitted'})

    response = client.post('/jobs', json={"start_date": "2025-01-01", "end_date": "2025-12-31"})
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['job_id'] == '123'

def test_create_job_missing_params(client):
    response = client.post('/jobs', json={})
    assert response.status_code == 200
    assert b'Error missing start_date or end_date' in response.data

# -------------------------------
# /jobs [GET]
# -------------------------------
def test_list_jobs_success(client, mocker):
    mocker.patch('your_api_file_name.jdb.keys', return_value=[b'jobid1'])

    response = client.get('/jobs')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert 'jobid1' in data

def test_list_jobs_none(client, mocker):
    mocker.patch('your_api_file_name.jdb.keys', return_value=[])

    response = client.get('/jobs')
    assert response.status_code == 200
    assert b'No job ID' in response.data

# -------------------------------
# /jobs/<jobid> [GET]
# -------------------------------
def test_get_job_success(client, mocker):
    mocker.patch('your_api_file_name.get_job_by_id', return_value={'job_id': '123', 'status': 'finished'})

    response = client.get('/jobs/123')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['job_id'] == '123'

def test_get_job_not_found(client, mocker):
    mocker.patch('your_api_file_name.get_job_by_id', return_value=None)

    response = client.get('/jobs/999')
    assert response.status_code == 200
    assert b'Error job not found' in response.data

# -------------------------------
# /data/max_diam/<max_diameter> [GET]
# -------------------------------
def test_query_diameter_success(client, mocker):
    mocker.patch('your_api_file_name.rd.keys', return_value=[b'2025-Jan-01 12:00'])
    mocker.patch('your_api_file_name.rd.get', return_value=json.dumps({'Maximum Diameter': 0.5}).encode('utf-8'))

    response = client.get('/data/max_diam/1.0')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert '2025-Jan-01 12:00' in data

# -------------------------------
# /data/biggest_neos/<count> [GET]
# -------------------------------
def test_find_biggest_neo_success(client, mocker):
    mocker.patch('your_api_file_name.rd.keys', return_value=[b'2025-Jan-01 12:00'])
    mocker.patch('your_api_file_name.rd.get', return_value=json.dumps({'H(mag)': 22.0}).encode('utf-8'))

    response = client.get('/data/biggest_neos/1')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert isinstance(data, list)

def test_find_biggest_neo_invalid(client):
    response = client.get('/data/biggest_neos/abc')
    assert response.status_code == 200
    assert b'error' in response.data

# -------------------------------
# /now/<count> [GET]
# -------------------------------
def test_get_timeliest_neos_success(client, mocker):
    mocker.patch('your_api_file_name.rd.keys', return_value=[b'2025-Jan-01 12:00'])
    mocker.patch('your_api_file_name.rd.get', return_value=json.dumps({'Close-Approach (CA) Date': '2025-Jan-01 12:00'}).encode('utf-8'))

    response = client.get('/now/1')
    assert response.status_code == 200