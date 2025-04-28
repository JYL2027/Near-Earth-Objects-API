import pandas as pd
import numpy as np

def create_min_diam_column(row):
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

def plot_neos(jid):
    dat = {}

    job_info = json.loads(jdb.get(jid).decode('utf-8'))
    # extract start and end parameters
    month = job_info['month']
    year = job_info['year']

    for key in rd.keys('*'):
        key = key.decode('utf-8')
        if (key.split('-')[0] == year) and (key.split('-')[1] == month):
            try:
                val = json.loads(rd.get(key).decode('utf-8'))
                dat[key] = val
            except:
                logging.error(f'Error retrieving data at {key}')
    pass
    

    