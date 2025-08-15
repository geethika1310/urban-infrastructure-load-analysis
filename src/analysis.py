from db_connection import connect_db
import pandas as pd

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

def fetch_daily_usage():
    conn = connect_db()
    query = """
        SELECT usage_date, SUM(kWh) AS total_kWh
        FROM electricity_usage
        GROUP BY usage_date
        ORDER BY usage_date;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def fetch_occupancy_vs_usage():
    conn = connect_db()
    query = """
        SELECT b.building_id, b.occupancy, SUM(e.kWh) AS total_kWh
        FROM buildings b
        JOIN electricity_usage e ON b.building_id = e.building_id
        GROUP BY b.building_id, b.occupancy;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def fetch_zone_enriched():
    conn = connect_db()
    query = """
        SELECT z.zone, z.avg_temp, z.population_density, SUM(e.kWh) AS total_kWh
        FROM zone_info z
        JOIN buildings b ON z.zone = b.zone
        JOIN electricity_usage e ON b.building_id = e.building_id
        GROUP BY z.zone, z.avg_temp, z.population_density;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df
