import pytest
from worker import clean_to_date_only, parse_date
from datetime import datetime
from unittest import mock
import worker

# ----------------
# Unit tests for clean_to_date_only
# ----------------

def test_clean_to_date_only_basic():
    """Test standard cleaning of a full timestamp."""
    assert clean_to_date_only('2024-Apr-01 12:34') == '2024-Apr-01'

def test_clean_to_date_only_plus_minus():
    """Test cleaning a time string with a ± symbol."""
    assert clean_to_date_only('2024-Apr-01 12:34 ± 0.1 days') == '2024-Apr-01'

def test_clean_to_date_only_empty():
    """Test handling of an empty string."""
    assert clean_to_date_only('') == ' '

def test_clean_to_date_only_no_time():
    """Test input that is just a date, no time."""
    assert clean_to_date_only('2024-Apr-01') == '2024-Apr-01'

def test_clean_to_date_only_stripped_spaces():
    """Test handling of extra spaces."""
    assert clean_to_date_only(' 2024-Apr-01  ') == '2024-Apr-01'

# ----------------
# Unit tests for parse_date
# ----------------

def test_parse_date_valid():
    """Test parsing a correctly formatted date string."""
    assert parse_date('2024-Apr-01') == datetime(2024, 4, 1)

def test_parse_date_invalid_format():
    """Test parsing an incorrectly formatted date string raises ValueError."""
    with pytest.raises(ValueError):
        parse_date('April 1, 2024')

def test_parse_date_empty():
    """Test parsing an empty date string raises ValueError."""
    with pytest.raises(ValueError):
        parse_date('')

def test_parse_date_extra_spaces():
    """Test parsing date with extra spaces."""
    assert parse_date(' 2024-Apr-01 ') == datetime(2024, 4, 1)

# ----------------
# Testing do_work with mocks
# ----------------

@mock.patch('worker.update_job_status')
@mock.patch('worker.jdb')
@mock.patch('worker.rd')
@mock.patch('worker.plt')
@mock.patch('worker.open', new_callable=mock.mock_open, read_data=b'mock_image_data')
def test_do_work_success(mock_open, mock_plt, mock_rd, mock_jdb, mock_update_job_status):
    """Test that do_work runs successfully under ideal conditions."""

    # Mock job retrieval
    jobid = 'fake-job-id'
    job_data = {
        'start': '2024-Apr-01',
        'end': '2024-Apr-05'
    }
    mock_jdb.get.return_value = json.dumps(job_data).encode('utf-8')

    # Mock rd.keys and rd.get
    mock_rd.keys.return_value = [b'key1', b'key2']

    # Fake NEO data
    neo1 = {
        'Close-Approach (CA) Date': '2024-Apr-02 13:00',
        'V relative(km/s)': '25',
        'CA DistanceNominal (au)': '0.042'
    }
    neo2 = {
        'Close-Approach (CA) Date': '2024-Apr-03 16:00',
        'V relative(km/s)': '30',
        'CA DistanceNominal (au)': '0.039'
    }

    def fake_rd_get(key):
        if key == b'key1':
            return json.dumps(neo1).encode('utf-8')
        if key == b'key2':
            return json.dumps(neo2).encode('utf-8')
        return None

    mock_rd.get.side_effect = fake_rd_get

    # Call do_work
    worker.do_work(jobid)

    # Check updates
    mock_update_job_status.assert_any_call(jobid, "in progress")
    mock_update_job_status.assert_any_call(jobid, "complete")

    # Ensure file was read
    mock_open.assert_called_with(f'/app/{jobid}_plot.png', 'rb')

    # Ensure plot saving was called
    mock_plt.savefig.assert_called()

@mock.patch('worker.update_job_status')
@mock.patch('worker.jdb')
def test_do_work_missing_job_data(mock_jdb, mock_update_job_status):
    """Test do_work when job data is missing in Redis."""
    jobid = 'missing-job-id'
    mock_jdb.get.return_value = None

    with pytest.raises(ValueError, match="Job data not found in Redis"):
        worker.do_work(jobid)

@mock.patch('worker.update_job_status')
@mock.patch('worker.jdb')
def test_do_work_invalid_date(mock_jdb, mock_update_job_status):
    """Test do_work when date parsing fails."""
    jobid = 'bad-date-job-id'
    bad_job_data = {
        'start': 'bad-date',
        'end': '2024-Apr-05'
    }
    mock_jdb.get.return_value = json.dumps(bad_job_data).encode('utf-8')

    with pytest.raises(ValueError, match="Invalid job data: Unrecognized date format: bad-date"):
        worker.do_work(jobid)

