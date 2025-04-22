import pandas as pd

def create_new_diam_columns(level, row):
    if row.contains("±"):
        parts = row.split("±")
        max_num = parts[1].split()
        base = float(parts[0])
        offset = float(max_num[0])
        min_diam = base - offset
        max_diam = base + offset
        if(level == "Min"):
            return min_diam
        else:
            return max_diam
    else:
        dat = row.split()
        min = dat[0]
        max = dat[-2]
        if(level == "Min"):
            return min
        else:
            return max
