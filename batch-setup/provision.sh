#!/bin/bash

ASSETS_BUCKET='%(assets_bucket)s'

set +e
yum update -y
yum install -y git libgeos-devel python-devel postgresql96-devel gcc gcc-c++
for gocmd in batch-create-job-definition batch-submit-missing-meta-tiles missing-meta-tiles-read batch-tiles-split-low-high; do
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
export PLANET_DATE='%(planet_date)s'
export DATE_PREFIX='%(planet_date)s'
eof
