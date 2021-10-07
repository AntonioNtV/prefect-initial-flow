import csv

from prefect import task, Flow, Parameter
from logger import logger

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



def build_flow():
    with Flow("first-workflow") as flow:
        path = Parameter(name = "path", required = True)
        data = extract(path)
        t_data = transform(data)
        load(t_data, path)
    return flow

tasks_flow = build_flow()
tasks_flow.run(
    parameters={
        "path":"values.csv"
    }
)