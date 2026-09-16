from datetime import datetime, timedelta
import os
import sys
import json
import logging 

# Adds the 'src' folder to Python's search path before importing the local package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

# 1. Get the absolute path of the directory containing this DAG file
DAG_DIR = os.path.dirname(os.path.abspath(__file__))

# 2. Go up one level to the workspace root, then into the 'src' folder
SRC_DIR = os.path.abspath(os.path.join(DAG_DIR, "../src"))

# 3. Inject it into Python's search path if it's not already there
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook #hooks return objects that handle connection an
from nairobi_air_quality_batch_pipeline.api.openaq_client import OpenAQClient
from airflow.providers.postgres.hooks.postgres import PostgresHook



@dag(
    dag_id = "air_quality_dag",
    schedule = "@daily",  
    start_date = datetime(2026,1,1),
    catchup = False,
    default_args = {
        "owner":"lynn",
        "retries":5,
        "retry_delay": timedelta(minutes = 2),
    },
    tags = ["air_quality","nairobi","openaq"] 
) 


def nairobi_air_quality_pipeline_dag():

    @task
    def extract_locations():
        connection = BaseHook.get_connection("openaq_default")
        api_key = connection.extra_dejson.get("api_key")

        client = OpenAQClient(api_key = api_key, host = connection.host,)
        locations=client.get_locations(limit=1000)

        simplified_locations = [
            {"location_id":loc.get("id"), "name":loc.get("name")}
            for loc in locations
        ]
        print(f"Extracted{len(simplified_locations)} locations.")
        return simplified_locations


    @task
    def extract_sensors(location: dict) -> list[dict]:
    
        connection = BaseHook.get_connection("openaq_default")
        api_key = connection.extra_dejson.get("api_key")

        client = OpenAQClient(api_key=api_key, host=connection.host)
    
        location_id = location.get("location_id") or location.get("id")
        if not location_id:
            raise ValueError(f"Could not find an ID in the location data: {location}")
        
        sensors = client.get_sensors(

            location_id = location["location_id"]
            )

        normalized_sensors = [
            {
                "sensor_id": sensor["id"],
                "location_id": location["location_id"],
                "location_name": location["name"],
                "parameter": sensor["parameter"]["name"],
                "unit":sensor["parameter"]["units"],

            }
            for sensor in sensors
        ]

        print(f"Location {location.get('name', 'Unknown')} has {len(sensors)} sensors.")
    
        return normalized_sensors 



    @task
    def flatten_sensors(nested_sensors: list[list[dict]]) -> list[dict]:
        
        flat_list = [sensor for sensor_list in nested_sensors for sensor in sensor_list]
        
        print(f"Flattened {len(nested_sensors)} location lists into {len(flat_list)} total sensors.")
        return flat_list


    @task (max_active_tis_per_dag = 5)

    def extract_measurements(sensor: dict) -> list[dict]:

        connection = BaseHook.get_connection("openaq_default")
        api_key = connection.extra_dejson.get("api_key")

        client = OpenAQClient(api_key=api_key, host=connection.host)

        

        measurements = client.get_measurements(
            sensor_id=sensor["sensor_id"],
            limit=100,
        )
        
        normalized_measurements = [
            {
                "location_id": sensor["location_id"],
                "location_name": sensor["location_name"],
                "sensor_id": sensor["sensor_id"], 
                "parameter": sensor["parameter"],
                "unit": sensor["unit"],
                "value": measurement["value"],
                "measurement_timestamp": measurement["period"]["datetimeFrom"]["utc"],
            }
            for measurement in measurements
        ]
            
        print(f"Sensor {sensor['sensor_id']} extracted and normalized {len(normalized_measurements)} measurements.")
        return normalized_measurements


    @task
    def flatten_measurements(nested_measurements: list[list[dict]]) -> list[dict]:
        
        flat_list = [
            measurement for sublist in nested_measurements for measurement in sublist
            ]
        
        print(f"Flattened {len(nested_measurements)} sensor lists into {len(flat_list)} total measurements.")
        return flat_list


    #-------------------------------- validating measurement task ----------------------------------------

    @task
    def validate_measurements(

        measurements: list[dict],
        ) -> list[dict]:

        valid_measurements = []
        invalid_reasons = {
            "missing_location_id": 0,
            "missing_sensor_id": 0,
            "missing_parameter": 0,
            "missing_unit": 0,
            "missing_timestamp": 0,
            "invalid_value": 0,
            "negative_value": 0,
        }

        print(f"First measurement: {measurements[0]}")
        print(
            f"Value: {measurements[0].get('value')}, "
            f"type: {type(measurements[0].get('value'))}")


        for measurement in measurements:
            value = measurement.get("value")

            if measurement.get("location_id") is None:
                invalid_reasons["missing_location_id"] += 1
                continue

            if measurement.get("sensor_id") is None:
                invalid_reasons["missing_sensor_id"] += 1
                continue

            if not measurement.get("parameter"):
                invalid_reasons["missing_parameter"] += 1
                continue

            if not measurement.get("unit"):
                invalid_reasons["missing_unit"] += 1
                continue

            if not measurement.get("measurement_timestamp"):
                invalid_reasons["missing_timestamp"] += 1
                continue

            if not isinstance(value, (int, float)):
                invalid_reasons["invalid_value"] += 1
                continue

            if value < 0:
                invalid_reasons["negative_value"] += 1
                continue

            valid_measurements.append(measurement)
        invalid_count = sum(invalid_reasons.values())

        print(
        f"Validated measurements: "
        f"{len(valid_measurements)} valid, "
        f"{invalid_count} invalid"
        )

        print(f"Invalid reasons: {invalid_reasons}")

        return valid_measurements


    @task
    def load_measurements (measurements: list [dict]) -> None:

        if not measurements: 
            print("No measurements found!!")
            return

        postgres_hook = PostgresHook(postgres_conn_id = "postgres_default")

        sql = """

        INSERT INTO raw_air_quality (sensor_id, measurement_timestamp, location_id, location_name, parameter, unit, value)
        VALUES (%s, %s, %s, %s, %s, %s, %s) 
        ON CONFLICT (sensor_id, measurement_timestamp)
        DO UPDATE SET
            location_id = EXCLUDED.location_id,
            location_name = EXCLUDED.location_name,
            parameter = EXCLUDED.parameter,
            unit = EXCLUDED.unit,
            value = EXCLUDED.value;

        """ 
        rows = [
            (
                measurement["sensor_id"],
                measurement["measurement_timestamp"],
                measurement["location_id"],
                measurement["location_name"],
                measurement["parameter"],
                measurement["unit"],
                measurement["value"],
            )

            for measurement in measurements
        ]

        connection = postgres_hook.get_conn()
        cursor = connection.cursor()

        try:
            cursor.executemany(sql, rows)
            connection.commit()
            print(
                f"Successfully upserted "
                f"{len(rows)} measurements into PostgreSQL."
        )

        except Exception:
            connection.rollback()
            logging.exception("Failed to load measurements into PostgreSQL.")
            raise
        finally:
            cursor.close()
            connection.close()


    locations = extract_locations()
    sensors = extract_sensors.expand(location=locations)
    flat_sensors = flatten_sensors(nested_sensors=sensors)
    measurements = extract_measurements.expand(sensor=flat_sensors)
    flat_measurements = flatten_measurements(measurements)
    validated_measurements = validate_measurements(flat_measurements)
    load_measurements(validated_measurements)

nairobi_air_quality_pipeline_dag()
