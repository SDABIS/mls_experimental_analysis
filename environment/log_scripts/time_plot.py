import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
import numpy as np

plt.rcParams.update({
    "font.size": 14,
    "axes.titlesize": 16,
    "axes.labelsize": 14,
    "legend.fontsize": 13,
    "xtick.labelsize": 12,
    "ytick.labelsize": 12,
})

x_label = "num_users"
size_limit = 10000

def compare_r2_score(x, y):
    # Linear
    X = x.reshape(-1, 1)
    linear_model = LinearRegression()
    linear_model.fit(X, y)
    
    # Log
    X_log = np.log(x + 1).reshape(-1, 1)
    log_model = LinearRegression()
    log_model.fit(X_log, y)

    # Predict and score
    y_pred_linear = linear_model.predict(X)
    y_pred_log = log_model.predict(X_log)
    r2_linear = r2_score(y, y_pred_linear)
    r2_log = r2_score(y, y_pred_log)

    print(operation, line_names[i], r2_linear, r2_log)

def group_and_average(df):
    df = df.sort_values(by=x_label)  
    df[x_label] = (df[x_label] // 100) * 100
    return df.groupby(['group', x_label]).agg({
        'gen_elapsed_mean': 'mean',
        'processing_elapsed_mean': 'mean',
        'sizes_mean': 'mean',
    }).reset_index()

folder_path = "data"

operations = ['gen_elapsed_mean', "processing_elapsed_mean", "sizes_mean"]
y_labels = ['Generation time (milliseconds)', 'Processing time (milliseconds)', 'Size per update (KB)']

line_names = ["Commit", "2 Prop", "4 Prop", "8 Prop"]
files = ['commit.csv', 'prop_2.csv', "prop_4_2.csv", "prop_8.csv"]

#line_names = ["First", "Last", "Commit"]
#files = ["first.csv", "test_10000_opt_LAST.csv", "commit.csv"]

all_lines = []
for operation, y_label in zip(operations, y_labels):
    operation_lines = []
    plt.figure(figsize=(8, 5))

    for i in range(len(files)):
        # Load CSV
        file_path = folder_path + "/" + files[i]
        data = pd.read_csv(file_path)

        data = group_and_average(data)
        data = data[data[x_label] < size_limit]
        data = data[data[x_label] > 100]

        data[x_label] = pd.to_numeric(data[x_label], errors='coerce')
        data[operation] = pd.to_numeric(data[operation], errors='coerce')
        data[x_label] = data[x_label].fillna(0).astype(int)

        data[operation] = data[operation] / 1000  # Convert to milliseconds if time, or to KB if size

        #compare_r2_score(data[x_label].values, data[operation].sort_values)

        operation_lines.append(data[operation])
        plt.plot(data[x_label], data[operation], label=line_names[i], marker='.')

    plt.xlabel('Users')
    plt.ylabel(y_label)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("figures/" + operation + ".pdf")
    plt.show()

    all_lines.append(operation_lines)


print("\n")

for i in range(len(operations)):
    for j in range(len(line_names)):
        first_op = operations[i]

        second_op_index = (i+1) % len(operations)
        second_op = operations[second_op_index]

        line_name = line_names[j]
        first_data = all_lines[i][j]
        second_data = all_lines[second_op_index][j]

        corr = np.corrcoef(first_data, second_data)[0][1]

        print(f"{first_op} - {second_op} ({line_name}): {corr}")