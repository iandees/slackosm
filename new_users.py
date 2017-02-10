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

    return [lon_sum / n, lat_sum / n]


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

    for uid, obj in new_users:
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
    new_users = []

    logger.info("Starting at sequence %s", start_sqn)

    for verb, obj in iter_osm_stream(start_sqn=start_sqn):
        if isinstance(obj, pyosm.model.Finished):
            if new_users:
                update_feeds(new_users)
                push_existing_users(existing, obj.sequence)
                new_users = []
            logger.info("Finished processing sequence %s", obj.sequence)
            if (datetime.datetime.utcnow() - obj.timestamp).total_seconds() < 90:
                push_existing_users(existing, obj.sequence)
                logger.info("Done for now. Exiting.")
                break
            continue

        if obj.uid not in existing:
            logger.info("New user found: %s", obj.uid)
            new_users.append((obj.uid, obj))
            existing.add(obj.uid)


if __name__ == '__main__':
    main()
