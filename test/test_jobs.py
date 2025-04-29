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
    """Fixture to clean Redis databases before each test"""
    rd.flushdb()
    jdb.flushdb()
    rdb.flushdb()
    yield
    rd.flushdb()
    jdb.flushdb()
    rdb.flushdb()

def test_generate_jid():
    """Test job ID generation creates valid UUIDs"""
    jid = _generate_jid()
    assert isinstance(jid, str)
    assert len(jid) == 36
    # Verify it's a valid UUID
    uuid_obj = uuid.UUID(jid)
    assert str(uuid_obj) == jid

def test_instantiate_job():
    """Test job dictionary creation"""
    job = _instantiate_job("test123", "submitted", "HGNC:1", "HGNC:2")
    assert job == {
        'id': 'test123',
        'status': 'submitted',
        'start': 'HGNC:1',
        'end': 'HGNC:2'
    }

def test_save_and_get_job():
    """Test saving and retrieving a job"""
    test_job = {
        'id': 'test123',
        'status': 'submitted',
        'start': 'HGNC:1',
        'end': 'HGNC:2'
    }
    _save_job('test123', test_job)
   
    # Verify saved correctly
    saved_data = jdb.get('test123')
    assert saved_data is not None
    assert json.loads(saved_data) == test_job

def test_add_job():
    """Test adding a new job"""
    job = add_job("HGNC:1", "HGNC:2")
   
    assert isinstance(job, dict)
    assert 'id' in job
    assert job['status'] == 'submitted'
   
    # Verify saved in Redis
    saved_data = jdb.get(job['id'])
    assert saved_data is not None
    assert json.loads(saved_data)['start'] == "HGNC:1"

def test_get_job_by_id():
    """Test retrieving job by ID"""
    # First add a test job
    test_job = {
        'id': 'test123',
        'status': 'submitted',
        'start': 'HGNC:1',
        'end': 'HGNC:2'
    }
    jdb.set('test123', json.dumps(test_job))
   
    # Test retrieval
    retrieved = get_job_by_id('test123')
    assert retrieved == test_job
   
    # Test non-existent job
    with pytest.raises(Exception):
        get_job_by_id('nonexistent')

def test_update_job_status():
    """Test updating job status"""
    # Add test job
    test_job = {
        'id': 'test123',
        'status': 'submitted',
        'start': 'HGNC:1',
        'end': 'HGNC:2'
    }
    jdb.set('test123', json.dumps(test_job))
   
    # Update status
    update_job_status('test123', 'processing')
   
    # Verify update
    updated = json.loads(jdb.get('test123'))
    assert updated['status'] == 'processing'
    assert updated['start'] == 'HGNC:1'  # Other fields unchanged

def test_job_result_storage():
    """Test storing and retrieving job results"""
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