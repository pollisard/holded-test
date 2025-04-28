import logging as log
import random
import requests
import json


def send_events(events):
    try:
        response = requests.post('http://localhost:8000/collect', json=events)
        response.raise_for_status()
        if response.status_code != 200:
            log.error("Failed to send event:", events)
    except Exception as e:
        log.error(e)


def generate_random_json_objects(filename):
    with open(filename, encoding="utf8") as file_obj:
        rnd: int = random.randint(1, 10)
        count: int = 0
        data = []
        for line in file_obj:
            try:
                json_object = json.loads(line)
                data.append(json_object)
                count += 1
                if count == rnd:
                    send_events(data)
                    data.clear()
                    count = 0
                    rnd = random.randint(1, 10)
            except json.JSONDecodeError as e:
                log.error(f"Error parsing JSON: {e}")


generate_random_json_objects("events.json")
