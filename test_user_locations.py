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