import json
import pandas as pd
from datetime import datetime
import csv

def load_environmental_data(filepath):
    data = []
    filepath = 'C:\\Users\\tomgr\\.cache\\kagglehub\\datasets\\adilshamim8\\temperature\\versions\\1\\temperature.csv'

    with open(filepath, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            try:
                entry = {
                    "year": int(row["Year"]),
                    "country": row["Country"].strip().strip("'"),
                    "avg_temperature": float(row["Avg_Temperature_degC"]),
                    "co2_emissions": float(row["CO2_Emissions_tons_per_capita"]),
                    "sea_level_rise": float(row["Sea_Level_Rise_mm"]),
                    "rainfall_mm": int(row["Rainfall_mm"]),
                    "population": int(float(row["Population"])),  
                    "renewable_energy_pct": float(row["Renewable_Energy_pct"]),
                    "extreme_weather_events": int(row["Extreme_Weather_Events"]),
                    "forest_area_pct": float(row["Forest_Area_pct"]),
                    "timestamp": datetime.now() 
                }
                data.append(entry)
            except ValueError as e:
                print(f"Skipping invalid row: {e}")

    return data



def load_yelp_data(filepath):
    data = []
    filepath = 'C:\\Users\\tomgr\\.cache\\kagglehub\\datasets\\yelp-dataset\\yelp-dataset\\versions\\4\\yelp_academic_dataset_checkin.json'
    with open(filepath, 'r') as file:
        for line in file:
            try:
                entry = json.loads(line.strip())
                business_id = entry.get('business_id')
                raw_dates = entry.get('date', "")
                date_list = [datetime.strptime(d.strip(), "%Y-%m-%d %H:%M:%S") for d in raw_dates.split(",") if d.strip()]
                data.append({
                    "business_id": business_id,
                    "dates": date_list
                })
            except (json.JSONDecodeError, ValueError) as e:
                print(f"Skipping line due to error: {e}")
    return data

def load_ev_population_data(filepath):
    filepath = 'C:\\Users\\tomgr\\.cache\\kagglehub\\datasets\\adarshde\\electric-vehicle-population-dataset\\versions\\1\\Electric_Vehicle_Population_Data.csv'
    with open(filepath, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)


