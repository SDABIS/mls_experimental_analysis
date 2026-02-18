import pandas as pd
import glob
import os
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

def compute_mean(row, list_col):
    y_list = row[list_col]

    if not isinstance(y_list, list):
        y_list = [] if pd.isna(y_list) else [y_list]

    return sum(y_list) / len(y_list)

def mean_scalar_list(df, scalar_col, list_col, result_col):
    def compute_mean_with_scalar(row):
        x = row[scalar_col]
        y_list = row[list_col]

        if not isinstance(y_list, list):
            y_list = [] if pd.isna(y_list) else [y_list]
        
        # Combine values
        values = [x] + y_list if not pd.isna(x) else y_list
        return sum(values) / len(values) if values else None

    df[result_col] = df.apply(compute_mean_with_scalar, axis=1)
    return df

# Calculate latency from timestamps
def calculate_latency(row):
    epoch_timestamp = row['epoch_generating_timestamp']
    if pd.isna(epoch_timestamp) or not isinstance(row['other_timestamps'], list):
        return []
    
    if isinstance(row['welcome_timestamps'], list):
        return [ts - epoch_timestamp for ts in (row['other_timestamps'] + row['welcome_timestamps']) if pd.notna(ts)]
    else:
        return [ts - epoch_timestamp for ts in row['other_timestamps'] if pd.notna(ts)]

def calculate_latency_statistics(latencies):
    if len(latencies) == 0:
        return pd.Series([None, None], index=['mean_latency', 'max_latency'])
    return pd.Series([pd.Series(latencies).mean(), pd.Series(latencies).max()],
                     index=['mean_latency', 'max_latency'])

#cost per update
def calculate_mean_size(row):
    if row['epoch'] == 0 or row['number_of_proposals'] == 0: return 0
    return (row['propose_sizes'] * row['number_of_proposals'] + row['next_size']) / (row['number_of_proposals'])

def calculate_processing_elapsed(row):
    if row['epoch'] == 0 or row['number_of_proposals'] == 0: return 0
    return (row['storeprop_times'] * row['number_of_proposals'] + row['next_update_process_time']) / (row['number_of_proposals'])

def calculate_gen_elapsed(row):
    if row['epoch'] == 0 or row['number_of_proposals'] == 0: return 0
    return (row['propose_times'] * row['number_of_proposals'] + row['next_update_gen_time']) / (row['number_of_proposals'])


pd.options.display.max_columns = None
log_directory = '../client/logs'

options = ["outliers", "ignore_zeros"]
log_files = glob.glob(os.path.join(log_directory, '*.txt'))

data_frames = []

def read_log_file(file):
    data = []
    with open(file, 'r') as f:
        for line in f:
            try: 
                parts = line.split()

                if len(parts) < 5:
                    continue
                # 5th element determines the operation
                operation = parts[4]

                # Default values
                size = 0
                other_size = 0
                target_user_id = None
                # Each operation is composed of different values
                if operation in ['Create']:
                    group, epoch, num_users, user_id, operation, timestamp, elapsed = parts
                    number_of_proposals = 1

                elif operation in ['Commit']:
                    group, epoch, num_users, user_id, operation, number_of_proposals, size, timestamp, elapsed = parts

                # Compatibility with deep analysis
                elif operation in ['DeepCommit']:
                    group, epoch, num_users, user_id, operation, size, _bars, _, _, _, _, _, _, _, _, _bars_2, _, _, _bars_3, _, _bars_4, _, _, _, timestamp, elapsed = parts
                    number_of_proposals = 1
                # Compatibility with deep analysis
                elif operation in ['DeepProcess']:
                    group, epoch, num_users, user_id, operation, _bars, target_user_id, _, _, _, _, _, _, _, _, _bars_2, _, _, _, timestamp, elapsed = parts

                elif operation in ['Update']:  
                    group, epoch, num_users, user_id, operation, size, timestamp, elapsed = parts
                    number_of_proposals = 1

                elif operation in ['Join']:
                    group, epoch, num_users, user_id, operation, size, other_size, timestamp, elapsed = parts
                    number_of_proposals = 1

                elif operation in ['Invite']:
                    group, epoch, num_users, user_id, operation, target_user_id, size, timestamp, elapsed = parts
                    number_of_proposals = 1

                elif operation in ['Remove']:
                    if "ignore_remove" in options:
                        continue
                    group, epoch, num_users, user_id, operation, target_user_id, size, timestamp, elapsed = parts
                    number_of_proposals = 1

                elif operation in ['Process', 'StoreProp']:
                    if int(parts[-1]) == 0 and "ignore_zeros" in options:
                        continue
                    group, epoch, num_users, user_id, operation, target_user_id, timestamp, elapsed = parts
                    number_of_proposals = 0
                    
                elif operation in ['Welcome']:
                    group, epoch, num_users, user_id, operation, target_user_id, other_size, timestamp, elapsed = parts
                    number_of_proposals = 0

                elif operation in ['Propose']:
                    sub_operation = parts[5]
                    if sub_operation in ['Invite']:
                        group, epoch, num_users, user_id, operation, sub_operation, target_user_id, size, timestamp, elapsed = parts
                        number_of_proposals = 0

                    elif sub_operation in ['Remove']:
                        if "ignore_remove" in options:
                            continue
                        group, epoch, num_users, user_id, operation, sub_operation, target_user_id, size, timestamp, elapsed = parts
                        number_of_proposals = 0

                    elif sub_operation in ['Update']:
                        group, epoch, num_users, user_id, operation, sub_operation, size, timestamp, elapsed = parts
                        number_of_proposals = 0
                
                elif operation in ['CommitAttempt']:
                    sub_operation = parts[5]
                    
                    if sub_operation in ['Commit']:
                        group, epoch, num_users, user_id, operation, sub_operation, number_of_proposals, size, timestamp, elapsed = parts
                    
                    elif sub_operation in ['Invite']:
                        group, epoch, num_users, user_id, operation, sub_operation, target_user_id, size, timestamp, elapsed = parts
                        number_of_proposals = 1

                    elif sub_operation in ['Remove']:
                        if "ignore_remove" in options:
                            continue
                        group, epoch, num_users, user_id, operation, sub_operation, target_user_id, size, timestamp, elapsed = parts
                        number_of_proposals = 1

                    elif sub_operation in ['Update']:
                        group, epoch, num_users, user_id, operation, sub_operation, size, timestamp, elapsed = parts
                        number_of_proposals = 1

                    elif sub_operation in ['Join']:
                        group, epoch, num_users, user_id, operation, sub_operation, size, other_size, timestamp, elapsed = parts
                        number_of_proposals = 1

                else:
                    continue
                
                # add new line to dataframe
                data.append([group, int(epoch), int(num_users), user_id, operation, target_user_id, int(size), int(other_size), int(number_of_proposals), int(timestamp), int(elapsed)])
            except ValueError as e:
                print(line, e)

    # create dataframe with all lines
    df = pd.DataFrame(data, columns=["group", "epoch", "num_users", "user_id", "operation", "target_user_id", "size", "other_size", "number_of_proposals", "timestamp", "elapsed_time"])
    return df

# Parse all log files into a dataframe
for file in log_files:
    df = read_log_file(file)
    data_frames.append(df)

# Concatenate all dataframes
all_logs_df = pd.concat(data_frames, ignore_index=True)

epoch_generating_events = all_logs_df[all_logs_df['operation'].isin(['Commit', 'Update', 'Remove', 'Join', 'Create', 'Invite', 'DeepCommit'])]
commit_attempt_events = all_logs_df[all_logs_df['operation'].isin(['CommitAttempt'])]
propose_events = all_logs_df[all_logs_df['operation'].isin(['Propose'])]
process_events = all_logs_df[all_logs_df['operation'].isin(['Process', 'DeepProcess'])]
welcome_events = all_logs_df[all_logs_df['operation'].isin(['Welcome'])]
process_proposals_events = all_logs_df[all_logs_df['operation'].isin(['StoreProp'])]

# Group logs by group and epoch and rename columns
# Should only be 1 "epoch_generating_event" for group and epoch
epoch_generating_events_grouped = epoch_generating_events.groupby(['group', 'epoch']).agg({
    'num_users': 'first',
    'timestamp': 'min',
    'size': 'first',
    'elapsed_time': 'first',
    'other_size': 'max',
    'number_of_proposals': 'max',
}).reset_index()
epoch_generating_events_grouped.rename(columns={'timestamp': 'epoch_generating_timestamp'}, inplace=True)
epoch_generating_events_grouped.rename(columns={'other_size': 'join_size'}, inplace=True)
epoch_generating_events_grouped.rename(columns={'elapsed_time': 'original_gen_time'}, inplace=True)
epoch_generating_events_grouped.rename(columns={'size': 'original_size'}, inplace=True)

commit_attempt_events_grouped = commit_attempt_events.groupby(['group', 'epoch']).agg({
    'size': list,
    'elapsed_time': list,
}).reset_index()
commit_attempt_events_grouped.rename(columns={'size': 'commit_attempt_sizes'}, inplace=True)
commit_attempt_events_grouped.rename(columns={'elapsed_time': 'commit_attempt_elapsed'}, inplace=True)

propose_events_grouped = propose_events.groupby(['group', 'epoch']).agg({
    'size': 'mean',
    'elapsed_time': 'mean',
}).reset_index()
propose_events_grouped.rename(columns={'size': 'propose_sizes'}, inplace=True)
propose_events_grouped.rename(columns={'elapsed_time': 'propose_times'}, inplace=True)

process_events_grouped = process_events.groupby(['group', 'epoch']).agg({
    'timestamp': list,
    'elapsed_time': 'mean',
}).reset_index()
process_events_grouped.rename(columns={'timestamp': 'other_timestamps'}, inplace=True)
process_events_grouped.rename(columns={'elapsed_time': 'process_times'}, inplace=True)

welcome_events_grouped = welcome_events.groupby(['group', 'epoch']).agg({
    'timestamp': list,
    'elapsed_time': 'mean',
    'other_size': 'mean',
}).reset_index()
welcome_events_grouped.rename(columns={'timestamp': 'welcome_timestamps'}, inplace=True)
welcome_events_grouped.rename(columns={'other_size': 'welcome_size'}, inplace=True)
welcome_events_grouped.rename(columns={'elapsed_time': 'welcome_times'}, inplace=True)

process_proposals_events_grouped = process_proposals_events.groupby(['group', 'epoch']).agg({
    'elapsed_time': 'mean',
}).reset_index()
process_proposals_events_grouped.rename(columns={'elapsed_time': 'storeprop_times'}, inplace=True)

# Combine all dataframes
grouped_logs = pd.merge(epoch_generating_events_grouped, propose_events_grouped, on=['group', 'epoch'], how='left')
grouped_logs = pd.merge(grouped_logs, commit_attempt_events_grouped, on=['group', 'epoch'], how='left')
grouped_logs = pd.merge(grouped_logs, process_events_grouped, on=['group', 'epoch'], how='left')
grouped_logs = pd.merge(grouped_logs, process_proposals_events_grouped, on=['group', 'epoch'], how='left')
grouped_logs = pd.merge(grouped_logs, welcome_events_grouped, on=['group', 'epoch'], how='left')

grouped_logs['number_of_proposals'] = grouped_logs['number_of_proposals'].fillna(0).astype(int)
grouped_logs['num_users'] = grouped_logs['num_users'].fillna(1).astype(int)
grouped_logs['storeprop_times'] = grouped_logs['storeprop_times'].fillna(0).astype(int)
grouped_logs['propose_times'] = grouped_logs['propose_times'].fillna(0)
grouped_logs['propose_sizes'] = grouped_logs['propose_sizes'].fillna(0)

grouped_logs['latency'] = grouped_logs.apply(calculate_latency, axis=1)

if "outliers" in options:
    grouped_logs['latency'] = grouped_logs['latency'].apply(remove_outliers)

if "raw_logs" in options:
    # raw logs
    raw_propose_events_grouped = propose_events.groupby(['group', 'epoch']).agg({
        'size': list,
        'elapsed_time': list,
    }).reset_index()
    raw_propose_events_grouped.rename(columns={'size': 'propose_sizes'}, inplace=True)
    raw_propose_events_grouped.rename(columns={'elapsed_time': 'propose_times'}, inplace=True)

    raw_process_events_grouped = process_events.groupby(['group', 'epoch']).agg({
        'elapsed_time': list,
    }).reset_index()
    raw_process_events_grouped.rename(columns={'elapsed_time': 'process_times'}, inplace=True)

    raw_welcome_events_grouped = welcome_events.groupby(['group', 'epoch']).agg({
        'elapsed_time': list,
        'other_size': list,
    }).reset_index()
    raw_welcome_events_grouped.rename(columns={'other_size': 'welcome_sizes'}, inplace=True)
    raw_welcome_events_grouped.rename(columns={'elapsed_time': 'welcome_times'}, inplace=True)

    raw_process_proposals_events_grouped = process_proposals_events.groupby(['group', 'epoch']).agg({
        'elapsed_time': list,
    }).reset_index()
    raw_process_proposals_events_grouped.rename(columns={'elapsed_time': 'storeprop_times'}, inplace=True)

    raw_startpoint = grouped_logs[['group', 'epoch', 'latency', 'original_gen_time', 'original_size', 'num_users', 'process_times']].copy()
    raw_startpoint.rename(columns={'process_times': 'process_times_mean'}, inplace=True)
    raw_grouped_logs = pd.merge(raw_startpoint, raw_propose_events_grouped, on=['group', 'epoch'], how='left')
    raw_grouped_logs = pd.merge(raw_grouped_logs, raw_process_events_grouped, on=['group', 'epoch'], how='left')
    raw_grouped_logs = pd.merge(raw_grouped_logs, raw_welcome_events_grouped, on=['group', 'epoch'], how='left')
    raw_grouped_logs = pd.merge(raw_grouped_logs, raw_process_proposals_events_grouped, on=['group', 'epoch'], how='left')

    raw_grouped_logs = raw_grouped_logs.groupby(['group', 'num_users']).agg({
        'propose_sizes': sum,
        'propose_times': sum,
        'process_times': sum,
        'welcome_times': sum,
        'welcome_sizes': sum,
        'storeprop_times': sum,
        'latency': sum,
        'process_times_mean': list,
        'original_gen_time': list,
        'original_size': list,
    }).reset_index()

    raw_grouped_logs.rename(columns={
        'original_gen_time': 'gen_time', 
        'original_size': 'commit_size'}, 
        inplace=True)

# Add mean an max latency columns in seconds
stats = grouped_logs['latency'].apply(calculate_latency_statistics)
grouped_logs = pd.concat([grouped_logs, stats], axis=1)
grouped_logs['mean_latency'] = pd.to_numeric(grouped_logs['mean_latency'], errors='coerce') / 1e3
grouped_logs['max_latency'] = pd.to_numeric(grouped_logs['max_latency'], errors='coerce') / 1e3

# order dataframe
grouped_logs = grouped_logs.sort_values(by=['group', 'epoch']).reset_index(drop=True)

grouped_logs = mean_scalar_list(grouped_logs, 'original_gen_time', 'commit_attempt_elapsed', 'gen_time')
grouped_logs = mean_scalar_list(grouped_logs, 'original_size', 'commit_attempt_sizes', 'size')

# add column with process times
grouped_logs['size'] = pd.to_numeric(grouped_logs['size'], errors='coerce')
grouped_logs['next_size'] = grouped_logs['size'].shift(-1).fillna(0)
grouped_logs['next_update_gen_time'] = grouped_logs['gen_time'].shift(-1).fillna(0)
grouped_logs['next_update_process_time'] = grouped_logs['process_times'].shift(-1).fillna(0)

grouped_logs['sizes_mean'] = grouped_logs.apply(calculate_mean_size, axis=1)
grouped_logs['processing_elapsed_mean'] = grouped_logs.apply(calculate_processing_elapsed, axis=1)
grouped_logs['gen_elapsed_mean'] = grouped_logs.apply(calculate_gen_elapsed, axis=1)

# Remove intermediate columns
grouped_logs = grouped_logs.drop(columns=['next_size'])
grouped_logs = grouped_logs.drop(columns=['next_update_gen_time'])
grouped_logs = grouped_logs.drop(columns=['next_update_process_time'])
grouped_logs = grouped_logs.drop(columns=['other_timestamps'])
grouped_logs = grouped_logs.drop(columns=['welcome_timestamps'])
grouped_logs = grouped_logs.drop(columns=['latency'])

# print and save`
print(grouped_logs)
grouped_logs.to_csv('grouped_logs.csv', index=False)

if "raw_logs" in options:
    raw_grouped_logs.to_csv('raw_grouped_logs.csv', index=False)