#Purpose: Python scripts for DB connection, analysis, and visualization
import mysql.connector

def connect_db():
    conn = mysql.connector.connect(
        host="localhost",
        user="your_username",
        password="your_password",
        database="infrastructure_db"
    )
    return conn
import pandas as pd
from db_connection import connect_db

def fetch_zone_usage():
    conn = connect_db()
    query = """
        SELECT b.zone, SUM(e.kWh) AS total_kWh
        FROM electricity_usage e
        JOIN buildings b ON e.building_id = b.building_id
        GROUP BY b.zone;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def fetch_water_per_occupant():
    conn = connect_db()
    query = """
        SELECT b.zone, ROUND(SUM(w.liters) / SUM(b.occupancy), 2) AS liters_per_person
        FROM water_usage w
        JOIN buildings b ON w.building_id = b.building_id
        GROUP BY b.zone;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df
