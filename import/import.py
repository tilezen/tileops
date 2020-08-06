from urllib.parse import urlparse
import argparse
import database
import datetime
import osm
import osm2pgsql
import requests


def assert_no_snapshot(run_id):
    """
    Exit with a helpful message is the snapshot already exists - we don't want
    to repeat all that work of importing!
    """

    import boto3
    import sys

    snapshot_id = 'postgis-prod-' + run_id
    rds = boto3.client('rds')
    if database.does_snapshot_exist(rds, snapshot_id):
        print(
            "A snapshot with ID %r already exists, suggesting that this "
            "import has already completed. If you are sure that you want "
            "to re-run this import in its entirety, please delete that "
            "snapshot first." % (snapshot_id))
        sys.exit(0)


def assert_run_id_format(run_id):
    import re
    import sys

    m = re.match('^[a-zA-Z0-9]([a-zA-Z0-9_-]*[a-zA-Z0-9])?$', run_id)
    if m is None:
        print("Run ID %r is badly formed. Run IDs may only contain ASCII "
              "letters and numbers, dashes and underscores. Dashes or "
              "underscores may not appear at the beginning or end of the "
              "run ID." % (run_id,))
        sys.exit(1)


parser = argparse.ArgumentParser(
    description='Automated Tilezen database import')
parser.add_argument('--date', help='Date of the data (i.e: OSM planet file) '
                    'to use. Defaults to the latest available. YYYY-MM-DD.')
parser.add_argument('--planet-url', help='Instead of downloading the planet '
                    'file at a given --date, download it from this URL. '
                    'Requires setting --run-id.')
parser.add_argument('--planet-md5-url', help='When using --planet-url, '
                    'optionally set this to download the MD5 checksum of '
                    'the file at --planet-url to check the md5sum. Leave '
                    'this unset to skip performing the md5sum.')
parser.add_argument('bucket', help="S3 Bucket to store flat nodes file in.")
parser.add_argument('region', help="AWS Region of the bucket to store "
                    "flat nodes.")
parser.add_argument('iam-instance-profile', help="IAM instance profile to "
                    "use for the database loading instance. Must have "
                    "access to the S3 bucket for storing flat nodes.")
parser.add_argument('database-password', help="The 'master user password' "
                    "to use when setting up the RDS database.")
parser.add_argument('--vector-datasource-version', default='master',
                    help='Version (git branch, ref or commit) to use when '
                    'setting up the database.')
parser.add_argument('--find-ip-address',
                    help='how to find ip address, <ipify|meta>')
parser.add_argument('--run-id', help='Distinctive run ID to give to '
                    'this build. Defaults to planet date YYMMDD.')

args = parser.parse_args()

if args.planet_url:
    planet_url = args.planet_url

    assert args.run_id, '--planet-url requires --run-id'
    run_id = args.run_id

    print("Downloading planet from %s" % planet_url)

    # set to empty string so it doesn't get serialized as 'None'
    planet_md5_url = args.planet_md5_url or ""
else:
    if args.date is None:
        planet_date = osm.latest_planet_date()
        print("Latest planet date is: %s" % planet_date.strftime('%Y-%m-%d'))
    else:
        planet_date = datetime.datetime.strptime(args.date, '%Y-%m-%d').date()

    planet_url = "http://s3.amazonaws.com/osm-pds/{planet_year}/planet-{planet_date}.osm.pbf".format(
        planet_year=planet_date.year,
        planet_date=planet_date.strftime('%y%m%d'),
    )
    planet_md5_url = "http://s3.amazonaws.com/osm-pds/{planet_year}/planet-{planet_date}.osm.pbf.md5".format(
        planet_year=planet_date.year,
        planet_date=planet_date.strftime('%y%m%d'),
    )

    run_id = args.run_id or planet_date.strftime('%y%m%d')
assert_run_id_format(run_id)

# calculate the filename to use when downloading the planet file
planet_file = urlparse(planet_url).path.rsplit("/", 1)[-1]

# if there's a snapshot already, then exit.
assert_no_snapshot(run_id)

# NOTE: getattr usage here is to work around the bug in argparse where it
# doesn't replace - with _ when setting positional argument attribute names.
# it works ok with optional arguments, though.
db = database.ensure_database(run_id, getattr(args, 'database-password'))

ip_addr = None
if args.find_ip_address == 'ipify':
    ip_addr = requests.get('https://api.ipify.org').text
elif args.find_ip_address == 'meta':
    ip_addr = requests.get(
        'http://169.254.169.254/latest/meta-data/public-ipv4').text
else:
    assert 0, '--find-ip-address <ipify|meta>'


osm2pgsql.ensure_import(
    run_id, planet_url, planet_md5_url, planet_file, db, getattr(args, 'iam-instance-profile'),
    args.bucket, args.region, ip_addr, args.vector_datasource_version)

database.take_snapshot_and_shutdown(db, run_id)
