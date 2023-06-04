import sys
import os
import json
import csv
import requests
from datetime import datetime
from bs4 import BeautifulSoup as BS

def processing_data(csv_file):
    distances, fuels, durations = [], [], []
    with open(csv_file, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            distances.append(float(row["distance"]))
            fuels.append(float(row["fuel_used"]))
            start_time = datetime.strptime(row["start_time"], '%Y-%m-%d %H:%M:%S')
            end_time = datetime.strptime(row['end_time'], '%Y-%m-%d %H:%M:%S')
            durations.append((end_time.hour + end_time.minute/60 + end_time.second/3600) - (start_time.hour + start_time.minute/60 + start_time.second/3600))
                   
    distance = sum(distances)
    fuel = sum(fuels)
    duration = sum(durations)
    return distance, fuel, duration


def get_vin_id():
    response = requests.get(url="https://vingenerator.org")
    if response.status_code == 200:
        html_source = response.text.strip()
        soup = BS(html_source, "html.parser")
        element = soup.find(class_="input")
        vin_id = element["value"]
        return vin_id
    else:
        raise ConnectionError

def generate_telemetry():
    os.chdir(os.path.dirname(os.path.abspath(__file__))) #This code will change the default working directory to your current working directory
    csv_file = "vehicle_data.csv"
    VIN_number = get_vin_id()
    vehicle_model = VIN_number[11:]
    total_distance, total_fuel, total_duration = processing_data(csv_file)
    trip_payload = {
        "VIN": VIN_number,
        "VehicleModel": vehicle_model,
        "tripsummary": {
            "Distance": total_distance,
            "Duration": total_duration,
            "Fuel": total_fuel
        }
    }
    print(trip_payload)
    with open ('trip-payload.json', 'w') as json_file:
        json.dump(trip_payload, json_file,indent=3)

if __name__ == "__main__":
    generate_telemetry()