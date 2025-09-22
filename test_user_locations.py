import json

user_locations = {}

with open('data/sample/listen_events_head.jsonl') as f:
    for line in f:
        event = json.loads(line)
        user = event['userId']
        # Define location as a tuple of (city, state, lat, lon)
        location = (event.get('city'), event.get('state'), event.get('lat'), event.get('lon'))
        if user not in user_locations:
            user_locations[user] = set()
        user_locations[user].add(location)

for user, locations in user_locations.items():
    if len(locations) > 1:
        print(f"User {user} has multiple locations: {locations}")
    #else:
    #    print(f"User {user} has a single location: {locations}")

user_levels = {}

with open("data/sample/listen_events_head.jsonl") as f:
    for line in f:
        event = json.loads(line)
        user_id = event.get("userId")
        level = event.get("level")
        if user_id is not None and level is not None:
            if user_id not in user_levels:
                user_levels[user_id] = set()
            user_levels[user_id].add(level)

for user_id, levels in user_levels.items():
    if len(levels) > 1:
        print(f"User {user_id} has changed levels: {levels}")