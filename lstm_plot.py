import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.collections import LineCollection
import numpy as np
import seaborn as sns

plt.rcParams['figure.dpi'] = 1000

PATH_DATA = "./data_lstm/"
# FILE_CLASS =  "prices_class.csv"
FILE_CLASS =  "data_lstm_ind_2_classes_q175.csv"

df_all = pd.read_csv(PATH_DATA + FILE_CLASS)
df_all['date'] = pd.to_datetime(df_all['date'])
# plot the APPL stock
check_isin = df_all['isin'].unique()[2]
# check_isin = "IT0005508921"
df_plot = df_all.loc[(df_all['isin'] == check_isin) & (df_all['date'].dt.year == 2017) & (~df_all['class'].isna())][['date', 'close', 'class']].copy()
df_plot['class'] = df_plot['class'].astype(int)

# create segments (line between 2 neighbour dates)
df_plot['date_num'] = mdates.date2num(df_plot['date'])
df_plot['close_future'] = df_plot['close'].shift(-1)
df_plot['date_future'] = df_plot['date_num'].shift(-1)
df_plot.dropna(inplace=True)
plot_np = df_plot[['date_num', 'close', 'date_future', 'close_future']].to_numpy() # (-1, 4)

segments = plot_np.reshape(-1, 2, 2)
labels = df_plot['class'].to_numpy()
lc = LineCollection(segments, cmap='RdYlGn', array=labels, linewidth=1)

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

