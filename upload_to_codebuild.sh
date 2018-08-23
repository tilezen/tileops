#!/bin/bash

PREFIX=""
TREEISH="HEAD"
KEY=""

usage() {
    cat <<EOF >&2
Usage: $0 -b BUCKET [-p PREFIX] [-t TREEISH] [-k KEY]
  BUCKET:  The S3 bucket to upload to.
  PREFIX:  The prefix within the bucket, if any.
  TREEISH: Git description of the version to upload, defaults to HEAD.
  KEY:     The key to use under the bucket and prefix. Defaults to "tileops-$DESC.zip" where DESC is built from the git version.
EOF
    exit 1
}

while getopts "b:p:t:k:" opt; do
    case $opt in
	b)
	    BUCKET=$OPTARG
	    ;;
	p)
	    PREFIX=$OPTARG
	    ;;
	t)
	    TREEISH=$OPTARG
	    ;;
	k)
	    KEY=$OPTARG
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

if [ -z "$BUCKET" ]; then
    echo "Bucket parameter is mandatory" >&2
    usage
fi

if [ -z "$KEY" ]; then
    DESCRIPTION=`git describe --tags --always`
    KEY="tileops-${DESCRIPTION}.zip"
fi

if [ -n "$PREFIX" ]; then
    PREFIX+="/"
fi

git archive --format zip $TREEISH | aws s3 cp - "s3://$BUCKET/$PREFIX$KEY"
