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
aws s3 cp "s3://${ASSETS_BUCKET}/tileops/py/bootstrap-requirements.txt" /usr/local/etc/py-requirements.txt
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

export DATE='%(date_iso)s'
export RUN_ID='%(run_id)s'
export META_DATE_PREFIX='%(meta_date_prefix)s'

export RAW_TILES_VERSION='%(raw_tiles_version)s'
export TILEQUEUE_VERSION='%(tilequeue_version)s'
export VECTOR_DATASOURCE_VERSION='%(vector_datasource_version)s'

export METATILE_SIZE='%(metatile_size)d'
eof

mkdir /tmp/awslogs
curl 'https://s3.amazonaws.com//aws-cloudwatch/downloads/latest/awslogs-agent-setup.py' -o /tmp/awslogs/awslogs-agent-setup.py
chmod +x /tmp/awslogs/awslogs-agent-setup.py
cat > /tmp/awslogs/awslogs.conf <<EOF
[general]
state_file = /var/awslogs/state/agent-state

[/var/log/cloud-init-output.log]
file = /var/log/cloud-init-output.log
log_group_name = /snapzen/tps
log_stream_name = {instance_id}
datetime_format = %%b %%d %%H:%%M:%%S
EOF
/tmp/awslogs/awslogs-agent-setup.py -n -r us-east-2 -c /tmp/awslogs/awslogs.conf

cat > /usr/local/bin/run.sh <<EOF
#!/bin/bash

. /usr/local/venv/bin/activate
. /usr/local/etc/planet-env.sh
export PATH=/usr/local/bin:$PATH

# stop on error
set -e
# echo commands before executing them (useful to check that the arguments are correct)
set -x

python -u /usr/local/src/tileops/import/import.py --find-ip-address meta --date \$DATE --run-id \$RUN_ID \$TILE_ASSET_BUCKET \$AWS_DEFAULT_REGION \
       \$TILE_ASSET_PROFILE_ARN \$DB_PASSWORD
python -u /usr/local/src/tileops/batch-setup/make_tiles.py --num-db-replicas 10 \$RUN_ID --missing-bucket \$MISSING_BUCKET \
       --meta-date-prefix \$META_DATE_PREFIX \$RAWR_BUCKET \$META_BUCKET \$DB_PASSWORD
python -u /usr/local/src/tileops/batch-setup/make_rawr_tiles.py --config enqueue-rawr-batch.config.yaml --key-format-type hash-prefix \
       \$RAWR_BUCKET \$RUN_ID \$MISSING_BUCKET
python -u /usr/local/src/tileops/batch-setup/make_meta_tiles.py --date-prefix \$META_DATE_PREFIX --missing-bucket \$MISSING_BUCKET \
       --key-format-type hash-prefix --metatile-size \$METATILE_SIZE \$RAWR_BUCKET \$META_BUCKET \$RUN_ID
EOF
chmod +x /usr/local/bin/run.sh

# start script in nohup to preserve logs, and disown it so that this script can exit but allow run.sh to continue.
cd /home/ec2-user
sudo -u ec2-user /usr/bin/nohup /usr/local/bin/run.sh &
disown %%1
