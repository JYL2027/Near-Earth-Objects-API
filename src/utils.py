import pandas as pd
import numpy as np
from datetime import datetime

def create_min_diam_column(row):
    '''
    This function extracts the minimum diameter from the diameter column
        Args:
            row (str): The diameter cell in the dataframe
        Returns:
            min (int) : The minimum diameter of the NEO
    '''
    
    if not pd.isna(row):
        if "±" in str(row):
            parts = str(row).split("±")
            max_num = parts[1].split()
            base = float(parts[0])
            offset = float(max_num[0])
            min_diam = base - offset
            return min_diam
        else:
            d = str(row).split()
            min = d[0]
            return min
    else:
        return np.nan
    
def create_max_diam_column(row):
    '''
    This function extracts the maximum diameter from the diameter column
        Args:
            row (str): The diameter cell in the dataframe
        Returns:
            min (int) : The maximum diameter of the NEO
    '''
    
    if not pd.isna(row):
        if "±" in str(row):
            parts = str(row).split("±")
            max_num = parts[1].split()
            base = float(parts[0])
            offset = float(max_num[0])
            max_diam = base + offset
            return max_diam        
        else:
            d = str(row).split()
            max_diam = d[-2]
            return max_diam
    else:
        return np.nan

def clean_to_date_only(time: str) -> str:
    ''' 
    Cleans a NEO time string and extracts only the date part.

    Args:
        time (str): The raw time string.

    Returns:
        str: The date (YYYY-MMM-DD).
    '''
    # logging.debug("Begin cleaning date")
    if not time:
        return " "

    # Remove anything after ± if it exists
    if '±' in time:
        time = time.split('±')[0].strip()

    # Split off the time part and keep only the date
    parts = time.split()
    # logging.debug("Completed cleaning date")
    if parts:
        return parts[0]  
    else:
        return time.strip() 

def parse_date(date_str: str) -> datetime:
    '''
    This function parces the date given to be a datetime object

    Args:
        date_str: The date to be parsed as a string
    
    Returns:
        Returns the date format as a datetime object
    '''
    try:
        # logging.debug("Parsing date and converting to datetime...") 
        return datetime.strptime(date_str.strip(), "%Y-%b-%d")
    except ValueError:
        raise ValueError(f"Unrecognized date format: {date_str}")