import argparse
import database
import datetime
import osm
import osm2pgsql
import requests


parser = argparse.ArgumentParser(
    description='Automated Tilezen database import')
parser.add_argument('--date', help='Date of the data (i.e: OSM planet file) '
                    'to use. Defaults to the latest available. YYYY-MM-DD.')
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

args = parser.parse_args()

if args.date is None:
    planet_date = osm.latest_planet_date()
    print "Latest planet date is: %s" % planet_date.strftime('%Y-%m-%d')
else:
    planet_date = datetime.datetime.strptime(args.date, '%Y-%m-%d').date()

# NOTE: getattr usage here is to work around the bug in argparse where it
# doesn't replace - with _ when setting positional argument attribute names.
# it works ok with optional arguments, though.
db = database.ensure_database(planet_date, getattr(args, 'database-password'))

ip_addr = None
if args.find_ip_address == 'ipify':
    ip_addr = requests.get('https://api.ipify.org').text
elif args.find_ip_address == 'meta':
    ip_addr = requests.get(
        'http://169.254.169.254/latest/meta-data/public-ipv4').text
else:
    assert 0, '--find-ip-address <ipify|meta>'


osm2pgsql.ensure_import(
    planet_date, db, getattr(args, 'iam-instance-profile'), args.bucket,
    args.region, ip_addr, args.vector_datasource_version)

database.take_snapshot_and_shutdown(db, planet_date)
