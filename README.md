slackosm
========

A Slack bot to help watch OSM for new users.

## How It Works

The process of announcing new users in a Slack channel is a two step process. First, a script ([`new_users.py`](https://github.com/iandees/slackosm/blob/master/new_users.py)) looks for new users via the [minutely diffs provided by OpenStreetMap](https://wiki.openstreetmap.org/wiki/Planet.osm/diffs#Minute.2C_Hour.2C_and_Day_Files_Organisation), generating a GeoJSON file with ~24 hours of new users. After that, a separate script ([`announce_new_users.py`](https://github.com/iandees/slackosm/blob/master/announce_new_users.py)) is run to read from that GeoJSON file and post to a Slack channel with a nicely-formatted message with the new users since the last time it ran. Both of these scripts run one after the other every 10 minutes on a Heroku scheduler.

### `new_users.py`

This script uses [`pyosm`](https://github.com/iandees/pyosm), an OSM feed parsing tool I wrote, to read the minutely diffs and look in a [bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) stored on S3 to see if any of the users present in the diff are new.

For each new user, the script generates a point to represent their first edit, adds it to a GeoJSON file (chopping off any new users that are >24 hours old), and pushes that GeoJSON to S3. The process of finding a point to represent the user's first edit is a bit complex because of the way the OSM data model works. In most cases the user has added or edited a node, so we take that point as the location of their changeset. In some cases, though, the user's only change was to edit a way or delete something. In these cases, there's extra care we have to take to find a useful location.

### `announce_new_users.py`

This script reads the GeoJSON file generated above and uses an "incoming webhook" integration with Slack to send a message to a channel. That message includes the name of the user and a link to their first changeset on osm.org.
