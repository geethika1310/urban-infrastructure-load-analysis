import sys
sys.path.append('../src')

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from analysis import (
    fetch_zone_usage,
    fetch_water_per_occupant,
    fetch_daily_usage,
    fetch_occupancy_vs_usage,
    fetch_zone_enriched
)

zone_usage_df = fetch_zone_usage()
water_usage_df = fetch_water_per_occupant()

sns.barplot(data=zone_usage_df, x='zone', y='total_kWh')
plt.title('Electricity Usage by Zone')
plt.show()

sns.barplot(data=water_usage_df, x='zone', y='liters_per_person')
plt.title('Water Usage per Occupant')
plt.show()
