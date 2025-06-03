import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.collections import LineCollection
import numpy as np

PATH_DATA = "./data_lstm/"
# FILE_CLASS =  "prices_class.csv"
FILE_CLASS =  "data_lstm_ind_4_classes.csv"

df_all = pd.read_csv(PATH_DATA + FILE_CLASS)
df_all['date'] = pd.to_datetime(df_all['date'])
# plot the APPL stock
df_plot = df_all.loc[(df_all['isin'] == "AU0000119307") & (df_all['date'].dt.year >= 2020) & (~df_all['class'].isna())].copy()
df_plot['class'] = df_plot['class'].astype(int)


# Create segments
points = np.array([mdates.date2num(df_plot['date']), df_plot['close']]).T.reshape(-1, 1, 2)
segments = np.concatenate([points[:-1], points[1:]], axis=1)

# Create line collection colored by 'label' (average of segment endpoints)
labels_avg = (df_plot['class'].values[:-1] + df_plot['class'].values[1:]) / 2

lc = LineCollection(segments, cmap='RdYlGn', array=labels_avg, linewidth=2)

# Plot
fig, ax = plt.subplots()
ax.add_collection(lc)
ax.autoscale()
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
fig.autofmt_xdate()

# Add colorbar
cbar = plt.colorbar(lc, ax=ax)
cbar.set_label('Label Value')

plt.title('Value over Time Colored by Label')
plt.xlabel('Date')
plt.ylabel('Value')
plt.tight_layout()
plt.show()

