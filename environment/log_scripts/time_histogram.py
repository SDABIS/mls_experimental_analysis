import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
import numpy as np
import ast

def remove_outliers(latencies):
    if len(latencies) == 0:
        return latencies
    Q1 = pd.Series(latencies).quantile(0.25)
    Q3 = pd.Series(latencies).quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    return [latency for latency in latencies if lower_bound <= latency <= upper_bound]

num_users = 100
folder_path = "data"

operations = ['gen_time', "process_times_mean", "process_times", "commit_size", "number_of_ciphertexts"]
x_labels = ['Generation time (microseconds)', 'Processing time (microseconds)', 'Processing time (microseconds)', 'Size (Bytes)', 'Number of ciphertexts']

file = f'raw_grouped_logs_{num_users}.csv'

file_path = folder_path + "/" + file
data = pd.read_csv(file_path)

all_lines = []
for operation, x_label in zip(operations, x_labels):
    plt.figure(figsize=(8, 6))

    values = data[data['num_users'] == num_users][operation]
    values = values.apply(ast.literal_eval)
    values = values.explode() 
    values = values[values != 0].astype(float)

    values = remove_outliers(values)
    #percentile_99 = np.percentile(values, 99.9)
    #values = values[values < percentile_99]

    mean_val = np.mean(values)
    std_val = np.std(values)

    plt.hist(values, bins=25, edgecolor='black', density=True)

    print(f"{operation} - Mean: {mean_val:.2f}")
    print(f"{operation} - Standard Deviation: {std_val:.2f}")

    plt.xlabel(x_label)
    plt.ylabel("Frequency")

    plt.grid(True)
    plt.tight_layout()
    plt.savefig("figures/" + "histogram_" + operation + "_" + str(num_users) + ".pdf")
    plt.show()
