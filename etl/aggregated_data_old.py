import pandas as pd

# Load JSON files
auth = pd.read_json("auth_events.json", lines=True)
listen = pd.read_json("listen_events.json", lines=True)
page = pd.read_json("page_view_events.json", lines=True)
status = pd.read_json("status_change_events.json", lines=True)

# Example: Daily active users
listen['date'] = pd.to_datetime(listen['timestamp']).dt.date
daily_active = listen.groupby('date')['user_id'].nunique()

# Example: Plays per session
plays_per_session = listen.groupby('session_id')['song_id'].count().mean()

artists = pd.read_excel("Data Samples for TracktionAi project.xlsx", sheet_name="artists")
merged = listen.merge(artists, on="artist_id", how="left")

from datetime import datetime

auth['dob'] = pd.to_datetime(auth['dob'])
auth['age'] = auth['dob'].apply(lambda x: (datetime.now() - x).days // 365)

listen = listen.merge(auth[['user_id', 'age']], on='user_id', how='left')
age_distribution = listen['age'].value_counts().sort_index()
age_distribution = age_distribution / age_distribution.sum()
age_distribution = age_distribution.reset_index()
age_distribution.columns = ['age', 'proportion']
age_distribution = age_distribution.sort_values(by='age')
age_distribution.to_csv("age_distribution.csv", index=False)
age_distribution.plot(kind='bar', x='age', y='proportion', title='Age Distribution of Listeners')
# Save aggregated data
daily_active.to_csv("daily_active_users.csv")
with open("plays_per_session.txt", "w") as f:
    f.write(f"Average plays per session: {plays_per_session}\n")# Save aggregated data
daily_active.to_csv("daily_active_users.csv")
with open("plays_per_session.txt", "w") as f:
    f.write(f"Average plays per session: {plays_per_session}\n")
age_distribution.plot(kind='bar', x='age', y='proportion', title='Age Distribution of Listeners')