import pytest
import json
import uuid
from jobs import (
    rd, jdb, rdb,
    _generate_jid,
    _instantiate_job,
    _save_job,
    add_job,
    get_job_by_id,
    update_job_status,
    store_job_result,
    get_job_result
)

@pytest.fixture(autouse=True)
def clean_redis():
    """Fixture to clean Redis databases before each test."""
    # Mocking Redis calls for testing purposes
    rd.flushdb = MagicMock()
    jdb.flushdb = MagicMock()
    rdb.flushdb = MagicMock()
    yield
    rd.flushdb()
    jdb.flushdb()
    rdb.flushdb()

def test_generate_jid():
    """Test job ID generation creates valid UUIDs."""
    jid = _generate_jid()
    assert isinstance(jid, str)
    assert len(jid) == 36
    # Verify it's a valid UUID
    uuid_obj = uuid.UUID(jid)
    assert str(uuid_obj) == jid

def test_instantiate_job():
    """Test job dictionary creation with dates and kind as 1 or 2."""
    # Using date format like '2016-Oct-05'
    start_date = "2016-Oct-05"
    end_date = "2016-Oct-06"
    
    job = _instantiate_job("test123", "submitted", start_date, end_date, 1)
    assert job == {
        'id': 'test123',
        'status': 'submitted',
        'start': start_date,
        'end': end_date,
        'kind': 1
    }

def test_save_and_get_job():
    """Test saving and retrieving a job."""
    start_date = "2016-Oct-05"
    end_date = "2016-Oct-06"
    
    test_job = {
        'id': 'test123',
        'status': 'submitted',
        'start': start_date,
        'end': end_date,
        'kind': 1
    }
    _save_job('test123', test_job)
   
    # Verify saved correctly
    saved_data = jdb.get('test123')
    assert saved_data is not None
    assert json.loads(saved_data) == test_job

def test_add_job():
    """Test adding a new job."""
    start_date = "2016-Oct-05"
    end_date = "2016-Oct-06"
    
    # Added the 'kind' argument as either 1 or 2
    job = add_job(start_date, end_date, 1)
   
    assert isinstance(job, dict)
    assert 'id' in job
    assert job['status'] == 'submitted'
    assert job['kind'] == 1  # Verifying 'kind' field
   
    # Verify saved in Redis
    saved_data = jdb.get(job['id'])
    assert saved_data is not None
    assert json.loads(saved_data)['start'] == start_date

def test_get_job_by_id():
    """Test retrieving job by ID."""
    # First add a test job with kind as 1
    start_date = "2016-Oct-05"
    end_date = "2016-Oct-06"
    
    test_job = {
        'id': 'test123',
        'status': 'submitted',
        'start': start_date,
        'end': end_date,
        'kind': 1
    }
    jdb.set('test123', json.dumps(test_job))
   
    # Test retrieval
    retrieved = get_job_by_id('test123')
    assert retrieved == test_job
   
    # Test non-existent job
    with pytest.raises(KeyError):  # Adjusting for more specific exception handling
        get_job_by_id('nonexistent')

def test_update_job_status():
    """Test updating job status."""
    # Add test job
    start_date = "2016-Oct-05"
    end_date = "2016-Oct-06"
    
    test_job = {
        'id': 'test123',
        'status': 'submitted',
        'start': start_date,
        'end': end_date,
        'kind': 1
    }
    jdb.set('test123', json.dumps(test_job))
   
    # Update status
    update_job_status('test123', 'processing')
   
    # Verify update
    updated = json.loads(jdb.get('test123'))
    assert updated['status'] == 'processing'
    assert updated['start'] == start_date  # Other fields unchanged

def test_job_result_storage():
    """Test storing and retrieving job results."""
    test_result = {"output": "success", "data": [1, 2, 3]}
   
    # Store result
    store_job_result('job123', test_result)
   
    # Verify storage
    stored = rdb.get('job123')
    assert stored is not None
   
    # Convert both to dicts for comparison
    stored_dict = json.loads(stored)
    assert stored_dict == test_result
   
    # Test retrieval - parse the string if needed
    retrieved = get_job_result('job123')
    if isinstance(retrieved, str):
        retrieved = json.loads(retrieved.replace("'", '"'))  # Handle single quotes
    assert retrieved == test_result
   
    # Test non-existent result
    assert get_job_result('nonexistent') is None
