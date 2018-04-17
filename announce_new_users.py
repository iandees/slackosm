import arrow
import logging
import os
import requests
import redis
import sys

logger = logging.getLogger('osm')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)
redis_url = os.environ.get("REDIS_URL")
r = redis.from_url(redis_url) if redis_url else dict()

new_user_json_url = 'https://s3.amazonaws.com/data.openstreetmap.us/users/newest.json'
slack_url = os.environ.get('SLACK_WEBHOOK_URL')


def send_to_slack(message):
    logger.info("Telling Slack: %s", message)
    return requests.post(slack_url, json={'text': message})


def interesting_change(feature):
    props = feature.get('properties')
    contains = props.get('inside')

    if not contains:
        return False

    return contains.get('country_code') in ('us', 'ca')


features = requests.get(new_user_json_url).json()
features = features['features']

previous_timestamp = arrow.get(r.get('new_user_prev_timestamp'))
logger.info("Checking for new users since %s", previous_timestamp)

for feature in reversed(features):
    props = feature.get('properties')
    ts = arrow.get(props.get('timestamp'))
    if not previous_timestamp or ts > previous_timestamp:
        if not interesting_change(feature):
            continue

        inside = props.get('inside')
        if inside:
            locations = []
            for t in ('city', 'town', 'hamlet', 'locality', 'state'):
                locations.append(inside.get(t))
            locations = filter(None, locations)
            location_str = ', '.join(locations)
            location_str = u' in {}!'.format(location_str)
        else:
            location_str = '!'

        send_to_slack(
            u"`<https://www.openstreetmap.org/user/{username}|{username}>` just made "
            "their <https://www.openstreetmap.org/changeset/{changeset_id}|first edit>{location_str}"
            " (<https://osmcha.mapbox.com/changesets/{changeset_id}?filters="
            "%7B\"users\"%3A%5B%7B\"label\"%3A\"{username}\"%2C\"value\"%3A\"{username}\"%7D%5D%7D|OSMCha>)".format(
                username=props.get('user').get('name'),
                changeset_id=props.get('changeset').get('id'),
                location_str=location_str,
            )
        )

        previous_timestamp = ts

logger.info("Done with new users, latest time %s", previous_timestamp.isoformat())
r['new_user_prev_timestamp'] = previous_timestamp.isoformat()
