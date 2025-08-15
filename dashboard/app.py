import streamlit as st
from analysis import fetch_zone_usage, fetch_water_per_occupant

st.title("Urban Infrastructure Load Dashboard")

zone_df = fetch_zone_usage()
water_df = fetch_water_per_occupant()

st.subheader("Electricity Usage by Zone")
st.dataframe(zone_df)

st.subheader("Water Usage per Occupant")
st.dataframe(water_df)
