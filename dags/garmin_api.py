from geojson import LineString
import geopandas

from garth.exc import GarthException
import garth
import gpxpy

from datetime import datetime
import json
import os

from helpers import connect_to_db

from airflow.models import Variable

ACTIVITIES_FILE = "data/activities.json"


def login() -> None:
    if os.path.exists("~/.garth/oauth1_token.json"):
        garth.resume("~/.garth")
    try:
        garth.client.username

    except (GarthException, AssertionError):
        print("Session expired. Log in again")
        email = Variable.get("GARMIN_EMAIL")
        password = Variable.get("GARMIN_PASS")

        garth.login(email, password)
        garth.save("~/.garth")


def all_activities_to_file(filename=ACTIVITIES_FILE, start_date="2024-01-01", end_date="2024-06-14"):
    garth.resume("~/.garth")
    url = "/activitylist-service/activities/search/activities"
    start = 0
    limit = 20
    params = {
        "startDate": str(start_date),
        "endDate": str(end_date),
        "start": str(start),
        "limit": str(limit),
    }
    activities = []

    while True:
        params["start"] = str(start)
        act = garth.connectapi(url, params=params)
        if act:
            activities.append(act)
            start = start + limit
        else:
            break

    print(f"Found {len(act)} activities")

    with open(filename, "w", encoding='utf-8') as file:
        json.dump(activities[0], file, ensure_ascii=False, indent=4)
        file.close()


def get_activities_ids(filename):
    with open(filename, "r") as json_data:
        data = json.load(json_data)
        json_data.close()
    return [str(activity['activityId']) for activity in data]


def download_gpx():
    garth.resume("~/.garth")
    ids = get_activities_ids(ACTIVITIES_FILE)

    for activity_id in ids:
        if os.path.exists(f'data/activity_{activity_id}.gpx'):
            continue
        url = (
            f'/download-service/export/gpx/activity/{activity_id}'
        )
        gpx_content = garth.download(url)
        with open(f'data/activity_{activity_id}.gpx', "wb") as file:
            file.write(gpx_content)


def load_gpx_to_db():
    garth.resume("~/.garth")
    ids = get_activities_ids(ACTIVITIES_FILE)

    conn = connect_to_db()
    sql = conn.cursor()
    sql.execute("CREATE TABLE IF NOT EXISTS gpx_paths ("
                "id BIGINT PRIMARY KEY,"
                "create_date TIMESTAMP,"
                "line GEOMETRY(LINESTRING, 4326)"
                ");")

    for activity_id in ids:
        file_path = f'data/activity_{activity_id}.gpx'
        if not os.path.exists(file_path):
            continue

        with open(file_path, "r") as file:
            gpx = gpxpy.parse(file)
            file.close()

        gpx_points = gpx.tracks[0].segments[0].points
        if len(gpx_points) > 0:
            print("Send to db - ", activity_id)
            sql.execute("SELECT id FROM gpx_paths WHERE id = %s LIMIT 1;", (activity_id,))
            if sql.fetchone():
                continue

            ls = LineString([(point.latitude, point.longitude) for point in gpx_points])
            sql.execute("INSERT INTO gpx_paths (id, create_date, line) VALUES (%s, %s, %s);",
                        (int(activity_id), datetime.now(), str(ls)))
            conn.commit()
        else:
            os.remove(file_path)

    sql.close()
    conn.close()
