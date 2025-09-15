import pandas as pd

# Adjust the paths if needed
auth_sample = pd.read_json("auth_events.json", lines=True, nrows=10)
status_sample = pd.read_json("status_change_events.json", lines=True, nrows=10)
listen_sample = pd.read_json("listen_events.json", lines=True, nrows=10)
page_sample = pd.read_json("page_view_events.json", lines=True, nrows=10)

print(auth_sample)
print(status_sample)
print(listen_sample)
print(page_sample)

#what are the columns and data types in each file
print("Auth Events Columns:", auth_sample.columns.tolist())
print("Auth Events Dtypes:", auth_sample.dtypes.tolist())
print("Status Change Events Columns:", status_sample.columns.tolist())
print("Status Change Events Dtypes:", status_sample.dtypes.tolist())
print("Listen Events Columns:", listen_sample.columns.tolist())
print("Listen Events Dtypes:", listen_sample.dtypes.tolist())
print("Page View Events Columns:", page_sample.columns.tolist())
print("Page View Events Dtypes:", page_sample.dtypes.tolist())