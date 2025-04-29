import pytest
import numpy as np
from datetime import datetime
from your_module import (
    create_min_diam_column,
    create_max_diam_column,
    clean_to_date_only,
    parse_date
)

# ---- Tests for create_min_diam_column ----

def test_min_diam_with_uncertainty():
    assert create_min_diam_column("12.3 ± 0.4 km") == 11.9

def test_min_diam_without_uncertainty():
    assert create_min_diam_column("12.3 km") == "12.3"

def test_min_diam_with_nan():
    assert np.isnan(create_min_diam_column(np.nan))

# ---- Tests for create_max_diam_column ----

def test_max_diam_with_uncertainty():
    assert create_max_diam_column("12.3 ± 0.4 km") == 12.7

def test_max_diam_without_uncertainty():
    assert create_max_diam_column("12.3 km") == "12.3"

def test_max_diam_with_nan():
    assert np.isnan(create_max_diam_column(np.nan))

# ---- Tests for clean_to_date_only ----

def test_clean_date_only_standard():
    assert clean_to_date_only("2024-Jan-23 14:52") == "2024-Jan-23"

def test_clean_date_with_uncertainty():
    assert clean_to_date_only("2024-Jan-23 14:52 ± 00:01") == "2024-Jan-23"

def test_clean_date_empty():
    assert clean_to_date_only("") == " "

# ---- Tests for parse_date ----

def test_parse_valid_date():
    assert parse_date("2024-Jan-23") == datetime(2024, 1, 23)

def test_parse_date_with_extra_whitespace():
    assert parse_date(" 2024-Jan-23 ") == datetime(2024, 1, 23)

def test_parse_invalid_date():
    with pytest.raises(ValueError):
        parse_date("01/23/2024")
