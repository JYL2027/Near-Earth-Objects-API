import pytest
import json
from unittest import mock
import jobs

# ----------------
# Unit tests for _generate_jid
# ----------------

def test_generate_jid_unique():
    """Test that _generate_jid produces unique IDs."""
    jid1 = jobs._generate_jid()
    jid2 = jobs._generate_jid()
    assert isinstance(jid1, str)
    assert isinstance(jid2, str)
    assert jid1 != jid2

# ----------------
# Unit tests for _instantiate_job
# ----------------

def test_instantiate_job_creates_correct_dict():
    """Test that _instantiate_job returns correct job structure."""
    jid = "123"
    start = "2024-Apr-01"
    end = "2024-Apr-05"
    status = "submitted"
    job = jobs._instantiate_job(jid, status, start, end)
    
    assert job == {
        'id': jid,
        'status': status,
        'start': start,
        'end': end
    }

# ----------------
# Unit tests for _save_job
# ----------------

@mock.patch('jobs.jdb')
def test_save_job_stores_data(mock_jdb):
    """Test that _save_job saves a job dict in Redis."""
    jid = "test-jid"
    job_dict = {'id': jid, 'status': 'submitted', 'start': 'start', 'end': 'end'}
    
    jobs._save_job(jid, job_dict)
    mock_jdb.set.assert_called_once_with(jid, json.dumps(job_dict))

# ----------------
# Unit tests for _queue_job
# ----------------

@mock.patch('jobs.q')
def test_queue_job_puts_job(mock_q):
    """Test that _queue_job queues a job in HotQueue."""
    jid = "queue-jid"
    jobs._queue_job(jid)
    mock_q.put.assert_called_once_with(jid)

# ----------------
# Unit tests for add_job
# ----------------

@mock.patch('jobs._queue_job')
@mock.patch('jobs._save_job')
@mock.patch('jobs._instantiate_job')
@mock.patch('jobs._generate_jid')
def test_add_job_flow(mock_generate_jid, mock_instantiate_job, mock_save_job, mock_queue_job):
    """Test that add_job calls helper functions correctly."""
    mock_generate_jid.return_value = "new-jid"
    mock_instantiate_job.return_value = {'id': 'new-jid', 'status': 'submitted', 'start': 'start', 'end': 'end'}
    
    result = jobs.add_job("start", "end")
    
    assert result == {'id': 'new-jid', 'status': 'submitted', 'start': 'start', 'end': 'end'}
    mock_generate_jid.assert_called_once()
    mock_instantiate_job.assert_called_once()
    mock_save_job.assert_called_once_with("new-jid", result)
    mock_queue_job.assert_called_once_with("new-jid")

# ----------------
# Unit tests for get_job_by_id
# ----------------

@mock.patch('jobs.jdb')
def test_get_job_by_id_success(mock_jdb):
    """Test successful retrieval of a job by ID."""
    jid = "jobid"
    expected_job = {'id': jid, 'status': 'submitted'}
    mock_jdb.get.return_value = json.dumps(expected_job).encode('utf-8')
    
    job = jobs.get_job_by_id(jid)
    assert job == expected_job

# ----------------
# Unit tests for update_job_status
# ----------------

@mock.patch('jobs._save_job')
@mock.patch('jobs.get_job_by_id')
def test_update_job_status_success(mock_get_job_by_id, mock_save_job):
    """Test successful status update."""
    jid = "update-job"
    job_dict = {'id': jid, 'status': 'submitted', 'start': 'start', 'end': 'end'}
    mock_get_job_by_id.return_value = job_dict

    jobs.update_job_status(jid, "in progress")
    assert job_dict['status'] == "in progress"
    mock_save_job.assert_called_once_with(jid, job_dict)

@mock.patch('jobs.get_job_by_id')
def test_update_job_status_failure(mock_get_job_by_id):
    """Test update_job_status when no job is found."""
    mock_get_job_by_id.return_value = None
    with pytest.raises(Exception):
        jobs.update_job_status("bad-id", "failed")

# ----------------
# Unit tests for store_job_result
# ----------------

@mock.patch('jobs.rdb')
def test_store_job_result_success(mock_rdb):
    """Test storing a job result successfully."""
    job_id = "result-job"
    result = {"data": [1,2,3]}
    
    jobs.store_job_result(job_id, result)
    mock_rdb.set.assert_called_once_with(job_id, json.dumps(result))

@mock.patch('jobs.rdb')
def test_store_job_result_failure(mock_rdb):
    """Test storing a job result with Redis error."""
    mock_rdb.set.side_effect = Exception("Redis error")
    job_id = "bad-job"
    result = {"data": []}

    # Should print an error, but no crash
    jobs.store_job_result(job_id, result)

# ----------------
# Unit tests for get_job_result
# ----------------

@mock.patch('jobs.rdb')
def test_get_job_result_success(mock_rdb):
    """Test fetching job result successfully."""
    job_id = "good-result"
    expected_result = {"values": [4,5,6]}
    mock_rdb.get.return_value = json.dumps(expected_result).encode('utf-8')

    result = jobs.get_job_result(job_id)
    assert result == expected_result

@mock.patch('jobs.rdb')
def test_get_job_result_none(mock_rdb):
    """Test get_job_result when no result is found."""
    mock_rdb.get.return_value = None
    result = jobs.get_job_result("missing-result")
    assert result is None

@mock.patch('jobs.rdb')
def test_get_job_result_failure(mock_rdb):
    """Test get_job_result Redis failure."""
    mock_rdb.get.side_effect = Exception("Redis error")
    result = jobs.get_job_result("fail-result")
    assert result is None
