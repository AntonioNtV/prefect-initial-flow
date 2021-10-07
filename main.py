import csv

from prefect import task, Flow
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


with Flow("first-workflow") as flow:
    data = extract("values.csv")
    t_data = transform(data)
    load(t_data, "tvalues.csv")


flow.run()