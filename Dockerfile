FROM python:3.12

RUN mkdir /app
WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY src/jobs.py /app/jobs.py  
COPY src/NEO_api.py /app/NEO_api.py
COPY src/worker.py /app/worker.py
COPY src/neo.csv /app/neo.csv
COPY src/utils.py /app/utils.py
COPY test/test_jobs.py /app/test_jobs.py
COPY test/test_NEO_api.py /app/test_NEO_api.py
COPY test/test_worker.py /app/test_worker.py


ENV FLASK_APP=NEO_api.py