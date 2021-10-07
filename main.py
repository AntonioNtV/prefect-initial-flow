import csv
from datetime import datetime, timedelta

from prefect import task, Flow, Parameter
from logger import logger
from prefect.schedules import IntervalSchedule

@task
def extract(path):
    with open(path, "r") as file:
        text = file.readline().strip()
    data = [int(i) for i in text.split(",")]

    logger("extracted data: {data}".format(data = data))
    return data

@task
def transform(data):
    t_data = [i + 1 for i in data]
    logger("transformed data: {data}".format(data = t_data))
    return t_data

@task
def load(data, path):
    with open(path, 'w') as file:
       csv_writer = csv.writer(file)
       csv_writer.writerow(data)
    return



def build_flow(schedule = None):
    with Flow("first-workflow", schedule = schedule) as flow:
        path = Parameter(name = "path", required = True)
        data = extract(path)
        t_data = transform(data)
        load(t_data, path)
    return flow


tasks_flow_schedule = IntervalSchedule(
    start_date = datetime.now() + timedelta(seconds = 1),
    interval = timedelta(seconds = 5)
)

tasks_flow = build_flow(tasks_flow_schedule)
tasks_flow.run(
    parameters = {
        "path":"values.csv"
    }
)