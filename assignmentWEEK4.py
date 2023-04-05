import pandas as pd
import re
import matplotlib.pyplot as plt
from tqdm.auto import tqdm
tqdm.pandas()

# Open log file and load its data into a list of string
with open('short_access.txt', 'r') as log_file:
    raw_data = [line for line in log_file.readlines()]

# Convert raw_data strings list into data frame
df = pd.DataFrame(raw_data, columns = ['log_line'])

## PRE-PROCESSING
# Use regex pattern to parse each log line
# We use axis=1 to loop on each line and not column
pattern = r'^(\S+) (\S+) (\S+) \[(.*?)\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)" "(.*?)"$'
df['client_ip'] = df.progress_apply(lambda x: re.match(pattern, x['log_line']).group(1), axis = 1)
df['user'] = df.progress_apply(lambda x: re.match(pattern, x['log_line']).group(2), axis = 1)
df['http_auth_user'] = df.progress_apply(lambda x: re.match(pattern, x['log_line']).group(3), axis = 1)
df['timestamp'] = df.progress_apply(lambda x: re.match(pattern, x['log_line']).group(4), axis = 1)
df['request'] = df.progress_apply(lambda x: re.match(pattern, x['log_line']).group(5), axis = 1)
df['response_code'] = df.progress_apply(lambda x: re.match(pattern, x['log_line']).group(6), axis = 1)
df['response_size'] = df.progress_apply(lambda x: re.match(pattern, x['log_line']).group(7), axis = 1)
df['user_agent'] = df.progress_apply(lambda x: re.match(pattern, x['log_line']).group(9), axis = 1)

# Drop columns containing always the same value
# Drop log_line column (parsing done so its content is not useful now)
# Now, only : ['client_ip', 'timestamp', 'request', 'response_code', 'response_size', 'user_agent']
columns_to_drop = [column for column in df.columns if df[column].nunique() == 1]
columns_to_drop.append('log_line')
df = df.drop(columns_to_drop, axis = 1)

## USER AGENT ANALYSIS
# Create new dataframe specific for this analysis
agent_df = pd.DataFrame()

# Add the user agent raw date collected from previous parsing
agent_df['raw_data'] = df['user_agent']

# Parse and store browser from this raw_data
agent_df['browser'] = agent_df.progress_apply(lambda x: x['raw_data'].split('/')[0], axis = 1)

# Parse and store Operating System type
def get_os(agent_info: str):
    match = re.search(r'\((.*?)\)', agent_info)
    # Using only OS section
    if match:
        if 'Windows' in match.group(1):
            return 'Windows'
        elif 'iPhone' in match.group(1):
            return 'iPhone'
        elif 'Mac' in match.group(1):
            return 'Mac'
        elif 'Linux' in match.group(1):
            return 'Linux'
        elif 'Android' in match.group(1):
            return 'Android'
        # Barkrowler is a bot devoloped by eXenSa
        elif 'bot' or 'Barkrowler' in match.group(1):
            return 'bot'
        else:
            return '-'
    # If no match, no OS section detected so we use all the agent information
    else:
        if 'bot' or 'python' in agent_info:
            return 'bot'
        else:
            return '-'
agent_df['os'] = agent_df.progress_apply(lambda x: get_os(x['raw_data']), axis = 1)

# Count the number of occurrences of each OS
# Create a new dataframe from this computation
os_counts = agent_df['os'].value_counts()

## DISPLAY ANALYSIS
# Create a bar chart of the OS counts
os_counts.plot.bar()

# Set the chart title and axis labels
plt.title('User agent')
plt.xlabel('Operating System')
plt.ylabel('Number of users')

# Display the chart
plt.show()

## CSV WRITING
# Create a csv file for each dataframe
df.to_csv('df.csv', index=False)
agent_df.to_csv('agent_df.csv', index=False)
os_counts.to_csv('os_counts.csv', index=False)