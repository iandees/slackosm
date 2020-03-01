from pyosm.parsing import iter_osm_stream, iter_osm_file
from pybloom import ScalableBloomFilter
import arrow
import datetime
import pyosm.model
import boto3
import io
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

    f = io.BytesIO(obj['Body'].read())
    f.seek(0)

    bloom = ScalableBloomFilter.fromfile(f)

    start_sqn = obj['Metadata'].get('start_sequence_number')

    return bloom, int(start_sqn) if start_sqn else None


def push_existing_users(existing_user_bloom, sqn):
    f = io.ByteIO()
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

    if resp.status_code == 410:
        logger.info("Way %s was deleted, looking for most recent not-deleted version", way_id)

        resp = requests.get(
            'https://api.openstreetmap.org/api/0.6/way/{}/history'.format(way_id),
            stream=True,
        )
        resp.raw.decode_content = True
        visible_versions = list(filter(
            lambda o: o.visible,
            (obj for obj in iter_osm_file(resp.raw)),
        ))
        visible_version = visible_versions[-1]

        logger.info("Using version %s of way %s",
                    visible_version.version, visible_version.id)

        # Get the way version so we know the node IDs.
        # /way/<>/history doesn't give us nd elements
        resp = requests.get(
            'https://api.openstreetmap.org/api/0.6/way/{}/{}'.format(
                way_id, visible_version.version),
            stream=True,
        )
        resp.raw.decode_content = True
        visible_way = [obj for obj in iter_osm_file(resp.raw)][0]

        # Get the nodes for the way
        nodes_to_average = []
        for nd in visible_way.nds:
            logger.info("Looking for location of node %s", nd)
            resp = requests.get(
                'https://api.openstreetmap.org/api/0.6/node/{}/history'.format(
                    nd),
                stream=True,
            )
            resp.raw.decode_content = True

            # Pick the node version that was visible immediately
            # before the timestamp of the way
            keep_node = None
            for nd_hist in iter_osm_file(resp.raw):
                if nd_hist.visible and \
                   nd_hist.timestamp <= visible_way.timestamp:
                    keep_node = nd_hist

            logger.info("Using node %s/%s", keep_node.id, keep_node.version)
            nodes_to_average.append(keep_node)

    else:
        nodes_to_average = filter(
            lambda o: isinstance(o, pyosm.model.Node),
            (obj for obj in iter_osm_file(resp.raw)),
        )

    # Yes I know this is terrible.
    lat_sum = 0
    lon_sum = 0
    n = 0

    for obj in nodes_to_average:
        lat_sum += obj.lat
        lon_sum += obj.lon
        n += 1

    lon = lon_sum / n
    lat = lat_sum / n

    return {
        'type': 'Point',
        'coordinates': [lon, lat]
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
        'https://nominatim.openstreetmap.org/reverse',
        params=dict(format='json', lat='%0.6f' % lat, lon='%0.6f' % lon),
        headers={'User-Agent': 'slackosm (https://github.com/iandees/slackosm)'},
    )

    resp.raise_for_status()

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
            else:
                return 5
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
        gz = io.BytesIO()
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

        logger.info("Geometry: %s", geometry)
        pip = get_pip(geometry)
        address = pip.get('address') if pip else None
        if address:
            address.pop('road', None)
            properties['inside'] = address

        feature = {
            "type": "Feature",
            "properties": properties,
            "geometry": geometry,
        }

        existing_geojson.get('features').insert(0, feature)

    # Chop off the features more than a day old
    now = arrow.get()
    existing_geojson['features'] = [t for t in existing_geojson['features'] if (now-arrow.get(t['properties']['timestamp'])).days == 0]

    # TODO: Put together files of new users by day here?
    logger.info("Appending %s new users to new-users geojson", len(new_users))
    json_str = json.dumps(existing_geojson, separators=(',', ':'))
    json_bytes = json_str.encode('utf8')
    gz = io.BytesIO()
    gz_obj = gzip.GzipFile(fileobj=gz, mode='w')
    gz_obj.write(json_bytes)
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
            for uid, changes in user_to_objects.items():
                if uid not in existing:
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
