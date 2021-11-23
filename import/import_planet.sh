#!/bin/bash

PLANET_URL='%(planet_url)s'
PLANET_MD5_URL='%(planet_md5_url)s'
PLANET_FILE='%(planet_file)s'
PGPASSWORD='%(db_pass)s'
export PGHOST='%(db_host)s'
export PGPORT='%(db_port)d'
export PGDATABASE='%(db_name)s'
export PGUSER='%(db_user)s'
FLAT_NODES_BUCKET='%(flat_nodes_bucket)s'
FLAT_NODES_KEY='%(flat_nodes_key)s'
export AWS_DEFAULT_REGION='%(aws_region)s'
export OSM2PGSQL='/usr/bin/osm2pgsql'
VECTOR_DATASOURCE_VERSION='%(vector_datasource_version)s'

# we don't want the STATUS file moving around while we change working directories, especially if
# we have to report a failure.
SCRIPTPATH="$( cd "$(dirname "$0")"; pwd -P )"
STATUS="${SCRIPTPATH}/${0}.status"
PIDFILE="${SCRIPTPATH}/${0}.pid"

# check if we already finished
if [[ `cat $STATUS` == "finished" ]]; then
    echo "Already done."
    exit 0
fi

# or if we failed, leave the state as it is for someone to inspect
if [[ `cat $STATUS` == "failed" ]]; then
    echo "Failed, leaving state alone."
    exit 1
fi

# check if there's an already-running import
if [[ -f "${PIDFILE}" ]]; then
    pid=`cat "${PIDFILE}"`
    if kill -0 "$pid"; then
	echo "Import is running."
	cat $STATUS
	exit 0
    else
	# import terminated, but file remains
	rm "${PIDFILE}"
    fi
fi

# set up lockfile
echo $$ > "${PIDFILE}"
echo "starting up" > $STATUS

# to stop any apt install asking questions - NOTE: this doesn't get exported
# in any sudo environment!
export DEBIAN_FRONTEND=noninteractive

touch ~/.pgpass
chmod 0600 ~/.pgpass
echo "${PGHOST}:${PGPORT}:${PGDATABASE}:${PGUSER}:${PGPASSWORD}" > ~/.pgpass

# stop on any error, setting the status to failure so that we don't try again
# without someone taking a look to see why it failed.
function stop_with_failure {
    echo "failed" > $STATUS
    exit 1
}
trap stop_with_failure ERR

# software updating and installation is more-or-less idempotent
echo "installing software" > $STATUS
sudo DEBIAN_FRONTEND=noninteractive apt update
sudo DEBIAN_FRONTEND=noninteractive apt upgrade -y -q
sudo DEBIAN_FRONTEND=noninteractive apt install -y -q make g++ git awscli build-essential autoconf libtool pkg-config python-dev python3-pip python-pil libxml2-dev libxslt-dev unzip postgis osm2pgsql

# if there's no planet, then download it
if [[ ! -f "planet/${PLANET_FILE}" ]]; then
    echo "downloading planet" > $STATUS
    rm -rf planet
    mkdir planet
    cd planet/
    wget -q "${PLANET_URL}"

    if [[ -n "${PLANET_MD5_URL}" ]]; then
        wget -O "${PLANET_FILE}.md5" -q "${PLANET_MD5_URL}"
    fi

    cd ..
fi

# check the md5sum if the md5 file was downloaded
if [[ -f "planet/${PLANET_FILE}.md5" ]]; then
    echo "checking planet file MD5" > $STATUS
    (cd planet && md5sum --check "${PLANET_FILE}.md5")
else
    echo "skipping MD5 check"
fi

# check out vector-datasource
if [[ ! -d vector-datasource ]]; then
    echo "checking out vector-datasource" > $STATUS
    git clone https://github.com/tilezen/vector-datasource.git
    (cd vector-datasource && git checkout "${VECTOR_DATASOURCE_VERSION}")
fi

# set up database - it needs extensions adding prior to osm2pgsql run
echo "setting up database extensions" > $STATUS
psql -c "create extension if not exists postgis"
psql -c "create extension if not exists hstore"

# check if there's any data already in the database!
echo "checking for existing OSM data in database" > $STATUS
ntuples=`psql -t -c "select sum(n_live_tup) from pg_stat_user_tables where relname = 'planet_osm_polygon'"`
if [[ $ntuples -eq 0 ]]; then
    # no existing data => run osm2pgsql!
    echo "running osm2pgsql" > $STATUS
    (cd vector-datasource && $OSM2PGSQL --slim --hstore-all -C 61440 -S osm2pgsql.style -d "$PGDATABASE" -U "$PGUSER" -H "$PGHOST" --flat-nodes ../flat.nodes --number-processes 16 "../planet/${PLANET_FILE}")
fi

# if flat nodes file already exists in S3, don't upload it again
if aws s3 ls "s3://${FLAT_NODES_BUCKET}/${FLAT_NODES_KEY}"; then
    echo "Flat nodes file already exists - not uploading."

else
    # there should be a flat nodes file when all this is done
    if [[ ! -f flat.nodes ]]; then
	echo "Ran osm2pgsql, but there's no flat nodes file?"
	echo "failed" > $STATUS
	exit 1
    fi

    # it shouldn't be empty
    if [[ ! -s flat.nodes ]]; then
	echo "Empty flat nodes file. Did osm2pgsql fail? What does it say in nohup.log?"
	echo "failed" > $STATUS
	exit 1
    fi

    # otherwise, it looks okay. let's upload that file to S3 for safe-keeping.
    aws s3 cp flat.nodes "s3://${FLAT_NODES_BUCKET}/${FLAT_NODES_KEY}"
fi

# run the vector-datasource import steps
echo "checking for existing NE data in database" > $STATUS
ntuples=`psql -t -c "select sum(n_live_tup) from pg_stat_user_tables where relname = 'ne_10m_coastline'"`
if [[ $ntuples -eq 0 ]]; then
    if [[ ! -e env/bin/activate ]]; then
	echo "setting up python virtual env" > $STATUS
	virtualenv env --python python2.7
    fi
    source env/bin/activate

    echo "installing vector-datasource dependencies" > $STATUS
    cd vector-datasource
    pip install -U -r requirements.txt
    python setup.py develop

    echo "loading NE data into database" > $STATUS
    cd data

    python bootstrap.py
    make -f Makefile-import-data
    ./import-shapefiles.sh | psql -Xq -d $PGDATABASE

    echo "setting up min zooms and indexing" > $STATUS
    ./perform-sql-updates.sh -d $PGDATABASE --single-transaction
fi

ntuples=`psql -t -c "select sum(n_live_tup) from pg_stat_user_tables where relname = 'wof_neighbourhoods'"`
if [[ $ntuples -eq 0 ]]; then
    echo "loading WOF data into database" > $STATUS
    # if the WOF snapshot was included in the assets bundle, use that one. otherwise, use the older static dump.
    if [[ -f wof_snapshot.sql ]]; then
        psql -Xq -d $PGDATABASE -f wof_snapshot.sql
    else
        curl -so wof-neighbourhood.pgdump https://s3.amazonaws.com/tilezen-assets/wof/wof-neighbourhoods.pgdump
        pg_restore --clean -O < wof-neighbourhood.pgdump | psql -Xq -d $PGDATABASE
    fi
fi

# clean up!
echo "finished" > $STATUS
rm -f "${PIDFILE}"
