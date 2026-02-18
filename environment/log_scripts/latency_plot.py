import pandas as pd
import matplotlib.pyplot as plt
from scipy.ndimage  import gaussian_filter1d

x_label = "num_users"

def group_and_average(df):
    df = df.sort_values(by=x_label)
    df[x_label] = (df[x_label] // 100) * 100

    cols = ['mean_latency', 'max_latency']

    df.groupby(['group', x_label]).agg({
        'mean_latency': 'mean',
        'max_latency': 'mean',
    }).reset_index()

    df[cols] = df[cols].rolling(window=300, min_periods=1).mean()  # Apply rolling mean with a window of 3
    return df

folder_path = "data"

# Load CSV
file = 'commit.csv'
file_path = folder_path + "/" + file
data = pd.read_csv(file_path)
data = group_and_average(data)
data = data[data[x_label] > 100]
data = data[data[x_label] < 9900]

file_names = ['commit_1', 'commit_2', 'commit_3', 'prop_2_1', 'prop_2_2', 'prop_4_1', 'prop_4_2', 'prop_8_1', 'prop_8_2', 'prop_8_3']
datas = []
for file_name in file_names:
    file_path = folder_path + "/" + file_name + '.csv'
    data = pd.read_csv(file_path)
    data = group_and_average(data)
    data = data[data[x_label] > 100]
    data = data[data[x_label] < 9900]
    datas = datas + [data]
data = pd.concat(datas).groupby(x_label).agg({
    'mean_latency': 'mean',
    'max_latency': 'mean',
}).reset_index()


data[x_label] = pd.to_numeric(data[x_label], errors='coerce')
data['mean_latency'] = pd.to_numeric(data['mean_latency'], errors='coerce') / 1000  # Convert to milliseconds
data['max_latency'] = pd.to_numeric(data['max_latency'], errors='coerce') / 1000  # Convert to milliseconds
data[x_label] = data[x_label].fillna(0).astype(int)


# Create plot
plt.figure(figsize=(8, 6))
plt.plot(data[x_label], data['mean_latency'], label='Mean Latency', color='blue', marker='.')
plt.plot(data[x_label], data['max_latency'], label='Max Latency', color='red', marker='.')

plt.ticklabel_format(style='plain', axis='y')
plt.xlabel('Users')
plt.ylabel('Latency (Milliseconds)')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("figures/" + "latency.pdf")

plt.show()