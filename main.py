import csv

def extract(path):
    with open(path, "r") as file:
        text = file.readline().strip()
    data = [int(i) for i in text.split(",")]
    return data

def transform(data):
    t_data = [i + 1 for i in data]
    return t_data

def load(data, path):
    with open(path, 'w') as file:
       csv_writer = csv.writer(file)
       csv_writer.writerow(data)
    return

data = extract("values.csv")
t_data = transform(data)
load(t_data, "tvalues.csv")