#!/bin/bash

PREFIX=""
REGION="${AWS_DEFAULT_REGION}"
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

function title() {
    echo -e "\033[0;32m$*\033[0m"
}

usage() {
    cat <<EOF >&2
Usage: $0 -p PREFIX [-r REGION]
  PREFIX: The common bucket prefix. Buckets will be named like "\${PREFIX}-\${FUNCTION}-\${REGION}
  REGION: The AWS region to use. Defaults to \$AWS_DEFAULT_REGION.
EOF
    exit 1
}

while getopts "p:r:" opt; do
    case $opt in
        p)
            PREFIX=$OPTARG
            ;;
        r)
            REGION=$OPTARG
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            usage
            ;;
        :)
            echo "Option -$OPTARG requires an argument" >&2
            usage
            ;;
    esac
done

if [ -z "$PREFIX" ]; then
    echo "Prefix parameter is mandatory" >&2
    usage
fi

if [ -z "$REGION" ]; then
    echo "Please set the AWS region, either using the region argument or \$AWS_DEFAULT_REGION environment variable" >&2
    usage
fi

which go >/dev/null
if [ $? -ne 0 ]; then
    echo "Go compiler not found. Please install the go compiler." >&2
    usage
fi

TMPDIR=`mktemp -d`
function remove_tmp_dir {
    rm -rf "${TMPDIR}"
}
trap remove_tmp_dir EXIT

export GOPATH="${TMPDIR}"
export GOOS=linux
export GOARCH=amd64
export AWS_DEFAULT_REGION="${REGION}"

title "Creating S3 buckets"
for func in tile-assets rawr-tiles missing-tiles meta-tiles; do
   aws s3 mb "s3://${PREFIX}-${func}-${REGION}" --region "${REGION}"
done

title "Installing Go dependencies"
mkdir -p "${TMPDIR}/src/tzops"
ln -sT "${DIR}/../go" "${TMPDIR}/src/tzops/go"
go get github.com/aws/aws-sdk-go
go get gopkg.in/yaml.v2

title "Building static Go tools"
# NOTE: CGO_ENABLED=0 is provided to _not_ link the system C library. this is
# so that we don't get mismatches between most desktop Linux environments
# (which use GNU libc) and Alpine Linux (which uses MUSL).
(cd "${GOPATH}/src/tzops/go" && CGO_ENABLED=0 go install ./...)

title "Uploading Go tools to S3"
for i in tz-batch-create-job-definition \
             tz-missing-meta-tiles-write \
             tz-batch-submit-missing-meta-tiles \
             tz-missing-meta-tiles-read \
             tz-batch-tiles-split-low-high; do
    # Go will put the file in $GOPATH/bin if the GOOS & GOARCH match the host
    # machine, but will put it in $GOPATH/bin/$GOOS_$GOARCH/ if it doesn't!
    bin="${GOPATH}/bin/${i}"
    if [ ! -f "${bin}" ]; then
        bin="${GOPATH}/bin/${GOOS}_${GOARCH}/${i}"
    fi

    aws s3 cp "${bin}" "s3://${PREFIX}-tile-assets-${REGION}/tileops/go/${i}"
done

title "Uploading requirements.txt to S3"
cat > "${TMPDIR}/bootstrap-requirements.txt" <<EOF
Jinja2==2.9.6
MarkupSafe==1.0
ModestMaps==1.4.7
PyYAML==3.12
Shapely==1.6.2.post1
StreetNames==0.1.5
Werkzeug==0.12.2
appdirs==1.4.3
argparse==1.4.0
boto3==1.7.10
boto==2.48.0
edtf==2.6.0
enum34==1.1.6
future==0.16.0
hiredis==0.2.0
mapbox-vector-tile==1.2.0
paramiko==2.4.2
protobuf==3.4.0
psycopg2==2.7.3.2
pyclipper==1.0.6
pycountry==17.9.23
pyproj==1.9.5.1
python-dateutil==2.6.1
redis==2.10.6
requests==2.18.4
six==1.11.0
statsd==3.2.1
ujson==1.35
wsgiref==0.1.2
zope.dottedname==4.2
EOF
aws s3 cp "${TMPDIR}/bootstrap-requirements.txt" "s3://${PREFIX}-tile-assets-${REGION}/tileops/py/bootstrap-requirements.txt"

title "Creating a role for Codebuild TPS"
ACCOUNT_ID=`aws sts get-caller-identity --output text --query 'Account'`
read -r -d '' POLICY <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "VisualEditor0",
      "Effect": "Allow",
      "Action": [
        "iam:CreateInstanceProfile",
        "iam:GetRole",
        "secretsmanager:DescribeSecret",
        "secretsmanager:PutSecretValue",
        "iam:CreateRole",
        "ec2:RunInstances",
        "iam:AttachRolePolicy",
        "iam:PutRolePolicy",
        "iam:ListInstanceProfiles",
        "iam:AddRoleToInstanceProfile",
        "secretsmanager:UpdateSecret",
        "iam:PassRole",
        "secretsmanager:GetSecretValue"
      ],
      "Resource": [
        "arn:aws:iam::${ACCOUNT_ID}:role/tps-*",
        "arn:aws:iam::${ACCOUNT_ID}:role/ec2TilesAssetsRole",
        "arn:aws:iam::${ACCOUNT_ID}:instance-profile/tps-*",
        "arn:aws:iam::${ACCOUNT_ID}:instance-profile/ec2TilesAssetsRole",
        "arn:aws:secretsmanager:${REGION}:${ACCOUNT_ID}:secret:TilesDatabasePassword*",
        "arn:aws:ec2:*:*:subnet/*",
        "arn:aws:ec2:*:*:instance/*",
        "arn:aws:ec2:*:*:volume/*",
        "arn:aws:ec2:*:*:security-group/*",
        "arn:aws:ec2:*:*:network-interface/*",
        "arn:aws:ec2:*::image/*"
      ]
    },
    {
      "Sid": "VisualEditor1",
      "Effect": "Allow",
      "Action": "iam:GetInstanceProfile",
      "Resource": "arn:aws:iam::${ACCOUNT_ID}:instance-profile/*"
    },
    {
      "Sid": "VisualEditor2",
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeImages",
        "ec2:DescribeInstances",
        "ec2:CreateSecurityGroup",
        "secretsmanager:CreateSecret",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeInstanceStatus",
        "ec2:CreateTags"
      ],
      "Resource": "*"
    }
  ]
}
EOF
aws iam create-policy --policy-name AllowCodebuildToStartTPS --policy-document "${POLICY}"
