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



