from lxml import etree
from pyosm.parsing import iter_osm_stream, iter_osm_file
from pybloom import ScalableBloomFilter
import datetime
import pyosm.model
import boto3
import cStringIO as StringIO
import json
import logging
import sys
import requests
import gzip

logger = logging.getLogger('osm')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

s3 = boto3.client('s3')
existing_user_bucket = 'data.openstreetmap.us'
existing_user_key = 'users/existing_users.bloom'
new_users_key = 'users/newest.json'


def load_existing_users():
    obj = s3.get_object(
        Bucket=existing_user_bucket,
        Key=existing_user_key,
    )

    f = StringIO.StringIO(obj['Body'].read())
    f.seek(0)

    bloom = ScalableBloomFilter.fromfile(f)

    start_sqn = obj['Metadata'].get('start_sequence_number')

    return bloom, int(start_sqn) if start_sqn else None


def push_existing_users(existing_user_bloom, sqn):
    f = StringIO.StringIO()
    existing_user_bloom.tofile(f)
    f.seek(0)

    s3.upload_fileobj(
        f,
        existing_user_bucket,
        existing_user_key,
        ExtraArgs={
            "Metadata": {
                'start_sequence_number': str(sqn),
            }
        },
    )


def get_way_center(way_id):
    logger.info("Looking for center of way %s", way_id)
    resp = requests.get(
        'https://api.openstreetmap.org/api/0.6/way/{}/full'.format(way_id),
        stream=True,
    )
    resp.raw.decode_content = True

    # Yes I know this is terrible.
    lat_sum = 0
    lon_sum = 0
    n = 0

    for obj in iter_osm_file(resp.raw):
        if isinstance(obj, pyosm.model.Node):
            lat_sum += obj.lat
            lon_sum += obj.lon
            n += 1

    return {
        'type': 'Point',
        'coordinates': [lon_sum / n, lat_sum / n]
    }


def get_geometry(obj):
    if isinstance(obj, pyosm.model.Node):
        pt = {'type': 'Point', 'coordinates': [obj.lon, obj.lat]}
    elif isinstance(obj, pyosm.model.Way):
        return get_way_center(obj.id)
    elif isinstance(obj, pyosm.model.Relation):
        # TODO Get first node for the first member of the relation
        pt = None

    return pt


def get_pip(geometry):
    if not geometry:
        return None

    lat = geometry['coordinates'][1]
    lon = geometry['coordinates'][0]

    resp = requests.get(
        'https://pip.mapzen.com/',
        params=dict(latitude=lat, longitude=lon)
    )

    return resp.json()


def get_changeset(changeset_id):
    resp = requests.get(
        'https://api.openstreetmap.org/api/0.6/changeset/{}'.format(
            changeset_id
        ),
        stream=True,
    )
    resp.raw.decode_content = True
    cs = next(iter(iter_osm_file(resp.raw)))
    return cs


def find_representative_change(changes):
    # In a list of (verb, obj) tuples, find something that's useful
    # to represent the list of changes.

    # Order of preference:
    # 1. a node create/edit
    # 2. a way create/edit
    # 3. a node delete
    # 4. a relation

    def ordering_fn(tpl):
        if tpl[0] in ('create', 'modify'):
            if isinstance(tpl[1], pyosm.model.Node):
                return 1
            elif isinstance(tpl[1], pyosm.model.Way):
                return 2
        elif tpl[0] in ('delete',):
            if isinstance(tpl[1], pyosm.model.Node):
                return 3
            else:
                return 4
        else:
            return 5

    ordered_changes = sorted(changes, key=ordering_fn)

    return ordered_changes[0]


def update_feeds(new_users):
    try:
        gz = StringIO.StringIO()
        s3.download_fileobj(
            Bucket=existing_user_bucket,
            Key=new_users_key,
            Fileobj=gz,
        )
        gz.seek(0)
        gz_obj = gzip.GzipFile(fileobj=gz, mode='r')
        existing_geojson = json.load(gz_obj)
    except:
        logger.exception("Creating new users geojson for the first time")
        existing_geojson = {
            "type": "FeatureCollection",
            "features": [],
        }

    for uid, changes in new_users.items():
        verb, obj = find_representative_change(changes)
        geometry = get_geometry(obj)

        properties = {
            "user": {
                "id": obj.uid,
                "name": obj.user,
            },
            "timestamp": obj.timestamp.isoformat() + "Z",
        }

        cs = get_changeset(obj.changeset)
        if cs:
            properties['changeset'] = {
                "id": obj.changeset,
                "created_at": cs.created_at.isoformat() + "Z",
                "tags": dict([(t.key, t.value) for t in cs.tags]),
            }

        pip = get_pip(geometry)
        if pip:
            valid_places = filter(
                lambda p: p['Placetype'] in ('country', 'region', 'county', 'locality'),
                pip,
            )

            properties['inside'] = [
                {'type': p['Placetype'], 'wof:id': p['Id'], 'name': p['Name']}
                for p in valid_places
            ]

        feature = {
            "type": "Feature",
            "properties": properties,
            "geometry": geometry,
        }

        existing_geojson.get('features').insert(0, feature)

    # TODO: Prune off the last features if they're too old?
    # TODO: Put together files of new users by day here?
    logger.info("Appending %s new users to new-users geojson", len(new_users))
    # geojson = json.dumps(existing_geojson, separators=(',', ':'))
    gz = StringIO.StringIO()
    gz_obj = gzip.GzipFile(fileobj=gz, mode='w')
    json.dump(existing_geojson, gz_obj, separators=(',', ':'))
    gz_obj.close()
    gz.seek(0)
    s3.upload_fileobj(
        Fileobj=gz,
        Bucket=existing_user_bucket,
        Key=new_users_key,
        ExtraArgs={
            "ACL": 'public-read',
            "ContentType": 'application/json',
            "ContentEncoding": 'gzip',
        },
    )


def main():
    existing, start_sqn = load_existing_users()
    user_to_objects = {}

    logger.info("Starting at sequence %s", start_sqn)

    for verb, obj in iter_osm_stream(start_sqn=start_sqn):
        if isinstance(obj, pyosm.model.Finished):
            # Strip out any users that made changes this minute
            # who we've seen before
            for uid, changes in user_to_objects.items():
                if uid in existing:
                    del user_to_objects[uid]
                else:
                    existing.add(uid)
                    logger.info(
                        "New user %s found in changeset %s",
                        uid, changes[0][1].changeset
                    )

            # If we end up with users we haven't seen before,
            # add them to the feed of new users
            if user_to_objects:
                update_feeds(user_to_objects)
                push_existing_users(existing, obj.sequence)
                user_to_objects = {}

            logger.info("Finished processing sequence %s", obj.sequence)

            if (datetime.datetime.utcnow() - obj.timestamp).total_seconds() < 90:
                push_existing_users(existing, obj.sequence)
                logger.info("Done for now. Exiting.")
                break
            continue

        # Keep track of uid and their changes
        user_change_list = user_to_objects.get(obj.uid) or []
        user_change_list.append((verb, obj))
        user_to_objects[obj.uid] = user_change_list


if __name__ == '__main__':
    main()
