#!/bin/bash

ASSETS_BUCKET='%(assets_bucket)s'

declare -A VERSIONS
VERSIONS[raw_tiles]='%(raw_tiles_version)s'
VERSIONS[tilequeue]='%(tilequeue_version)s'
VERSIONS[vector-datasource]='%(vector_datasource_version)s'
VERSIONS[tileops]='%(tileops_version)s'

set +e
yum update -y
yum install -y git libgeos-devel python-devel postgresql96-devel gcc gcc-c++ docker
service docker start
usermod -a -G docker ec2-user
for gocmd in batch-create-job-definition batch-submit-missing-meta-tiles missing-meta-tiles-read missing-meta-tiles-write batch-tiles-split-low-high; do
    aws s3 cp "s3://${ASSETS_BUCKET}/tileops/go/tz-${gocmd}" "/usr/local/bin/tz-${gocmd}"
    chmod +x /usr/local/bin/tz-$gocmd
done

# embed requirements as heredoc. note that this used to be stored as a file on
# S3, but that caused a couple of issues when i forgot to update it. i noticed
# that my workflow for updating the requirements included searching this repo,
# so it seemed more likely that i'd catch the update if it was embedded here
# than remembering the separate step of updating the external file.
#
# as a bonus, it means that we can fork the repo and add/remove requirements on
# a per-branch basis without clobbering the (singleton) file on S3.
#
cat >/usr/local/etc/py-requirements.txt <<EOF
Jinja2==2.10.1
MarkupSafe==1.0
ModestMaps==1.4.7
PyYAML==4.2b4
Shapely==1.6.2.post1
StreetNames==0.1.5
Werkzeug==0.12.2
appdirs==1.4.3
argparse==1.4.0
boto3==1.9.32
boto==2.48.0
edtf==2.6.0
enum34==1.1.6
future==0.16.0
hiredis==0.2.0
mapbox-vector-tile==1.2.0
ModestMaps==1.4.7
mercantile==1.2.1
paramiko==2.4.2
protobuf==3.4.0
psycopg2==2.7.3.2
pyclipper==1.0.6
pycountry==17.9.23
pyproj==2.1.0
python-dateutil==2.6.1
redis==2.10.6
requests==2.20.1
six==1.11.0
statsd==3.2.1
ujson==1.35
wsgiref==0.1.2
zope.dottedname==4.2
EOF
pip install --upgrade pip
virtualenv /usr/local/venv
source /usr/local/venv/bin/activate
pip install -Ur /usr/local/etc/py-requirements.txt
cd /usr/local/src
for repo in raw_tiles tilequeue vector-datasource tileops; do
    git clone https://github.com/tilezen/$repo.git
    (cd $repo && git checkout ${VERSIONS[$repo]})
    if [ $repo != 'tileops' ]; then
        (cd $repo && python setup.py install)
    fi
    chown -R ec2-user:ec2-user $repo
done
cat > /usr/local/etc/planet-env.sh << eof
#!/bin/bash
export AWS_DEFAULT_REGION='%(region)s'
export TILE_ASSET_BUCKET='%(assets_bucket)s'
export TILE_ASSET_PROFILE_ARN='%(assets_profile_arn)s'
export DB_PASSWORD='%(db_password)s'
export RAWR_BUCKET='%(rawr_bucket)s'
export META_BUCKET='%(meta_bucket)s'
export MISSING_BUCKET='%(missing_bucket)s'

export PLANET_URL='%(planet_url)s'
export PLANET_MD5_URL='%(planet_md5_url)s'
export RUN_ID='%(run_id)s'
export META_DATE_PREFIX='%(meta_date_prefix)s'

export RAW_TILES_VERSION='%(raw_tiles_version)s'
export TILEQUEUE_VERSION='%(tilequeue_version)s'
export VECTOR_DATASOURCE_VERSION='%(vector_datasource_version)s'

export METATILE_SIZE='%(metatile_size)d'

export NUM_DB_REPLICAS='%(num_db_replicas)d'
export MAX_VCPUS='%(max_vcpus)d'
export JOB_ENV_OVERRIDES='%(job_env_overrides)s'
eof

mkdir /tmp/awslogs
curl 'https://s3.amazonaws.com//aws-cloudwatch/downloads/latest/awslogs-agent-setup.py' -o /tmp/awslogs/awslogs-agent-setup.py
chmod +x /tmp/awslogs/awslogs-agent-setup.py
cat > /tmp/awslogs/awslogs.conf <<EOF
[general]
state_file = /var/awslogs/state/agent-state

[/var/log/cloud-init-output.log]
file = /var/log/cloud-init-output.log
log_group_name = /tilezen/tps
log_stream_name = {instance_id}
datetime_format = %%b %%d %%H:%%M:%%S
EOF
/tmp/awslogs/awslogs-agent-setup.py -n -r "%(region)s" -c /tmp/awslogs/awslogs.conf

cat > /usr/local/bin/run.sh <<EOF
#!/bin/bash

. /usr/local/venv/bin/activate
. /usr/local/etc/planet-env.sh
export PATH=/usr/local/bin:$PATH

# stop on error
set -e
# echo commands before executing them (useful to check that the arguments are correct)
set -x

python -u /usr/local/src/tileops/import/import.py --find-ip-address meta --planet-url \$PLANET_URL --planet-md5-url \$PLANET_MD5_URL --run-id \$RUN_ID --vector-datasource-version \$VECTOR_DATASOURCE_VERSION \$TILE_ASSET_BUCKET \$AWS_DEFAULT_REGION \
       \$TILE_ASSET_PROFILE_ARN \$DB_PASSWORD
python -u /usr/local/src/tileops/batch-setup/make_tiles.py --num-db-replicas \$NUM_DB_REPLICAS \
       --max-vcpus \$MAX_VCPUS \$RUN_ID --missing-bucket \$MISSING_BUCKET \
       --meta-date-prefix \$META_DATE_PREFIX \$RAWR_BUCKET \$META_BUCKET \
       \$DB_PASSWORD --overrides \$JOB_ENV_OVERRIDES
python -u /usr/local/src/tileops/batch-setup/make_rawr_tiles.py --config enqueue-rawr-batch.config.yaml --key-format-type hash-prefix \
       \$RAWR_BUCKET \$RUN_ID \$MISSING_BUCKET
python -u /usr/local/src/tileops/batch-setup/make_meta_tiles.py --date-prefix \$META_DATE_PREFIX --missing-bucket \$MISSING_BUCKET \
       --key-format-type hash-prefix --metatile-size \$METATILE_SIZE \$RAWR_BUCKET \$META_BUCKET \$RUN_ID
EOF
chmod +x /usr/local/bin/run.sh


cat > /usr/local/bin/bbox_rebuild.sh <<EOF
#!/bin/bash

. /usr/local/venv/bin/activate
. /usr/local/etc/planet-env.sh
export PATH=/usr/local/bin:$PATH

# stop on error
set -e
# echo commands before executing them (useful to check that the arguments are correct)
set -x

python -u /usr/local/src/tileops/batch-setup/make_tiles.py --num-db-replicas $NUM_DB_REPLICAS --max-vcpus $MAX_VCPUS $RUN_ID --missing-bucket $MISSING_BUCKET --meta-date-prefix $META_DATE_PREFIX $RAWR_BUCKET $META_BUCKET $DB_PASSWORD --overrides $JOB_ENV_OVERRIDES

python -u /usr/local/src/tileops/batch-setup/make_rawr_tiles.py --config enqueue-rawr-batch.config.yaml --key-format-type hash-prefix --use-tile-coords-generator --tile-coords-generator-bbox=$BBOX $RAWR_BUCKET $RUN_ID $MISSING_BUCKET

python -u /usr/local/src/tileops/batch-setup/make_meta_tiles.py --date-prefix $META_DATE_PREFIX --missing-bucket $MISSING_BUCKET --key-format-type hash-prefix --metatile-size $METATILE_SIZE --use-tile-coords-generator --tile-coords-generator-bbox=$BBOX $RAWR_BUCKET $META_BUCKET $RUN_ID
EOF
chmod +x /usr/local/bin/bbox_rebuild.sh


# start script in nohup to preserve logs, and disown it so that this script can exit but allow run.sh to continue.
cd /home/ec2-user
sudo -u ec2-user /usr/bin/nohup /usr/local/bin/run.sh &
disown %%1
