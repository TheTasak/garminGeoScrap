import psycopg2


def connect_to_db():
    conn = psycopg2.connect(database="osm", user="postgres", host="localhost", port=5432)
    return conn
