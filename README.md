[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)


# Tileops
Operations information and scripts for dealing with the Tilezen infrastructure

The process has to go through three stages:

1. Importing geodata into an RDS PostgreSQL database and generating a snapshot.
2. Using replicas started from the snapshot to "render" RAWR tiles using AWS Batch.
3. Using the replicas and RAWR tiles to render meta tiles, suitable for use with Tapalcatl(-py).

The first thing to do, locally, is make sure that the Go commands have been built. See [the Go README](go/README.md) for details on how to do this. The Python environment will also need to be set up, see [the Python README](doc/python_setup.md). You will also need to have the [`raw_tiles`](https://github.com/tilezen/raw_tiles), [`tilequeue`](https://github.com/tilezen/tilequeue) and [`vector-datasource`](https://github.com/tilezen/vector-datasource) projects checked out as siblings to the `tileops` directory, i.e: accessible as `../raw_tiles/` and so forth from this README.

Rendering meta tiles can be done with the following commands:

```sh
python import/import.py --find-ip-address meta \
                        --date $DATE \
                        $TILE_ASSET_BUCKET \
                        $AWS_DEFAULT_REGION \
                        $TILE_ASSET_ROLE_ARN \
                        $DB_PASSWORD

python batch-setup/make_tiles.py --num-db-replicas 10 \
                                 $PLANET_DATE \
                                 $RAWR_BUCKET \
                                 $META_BUCKET \
                                 $DB_PASSWORD

python batch-setup/make_rawr_tiles.py --config enqueue-rawr-batch.config.yaml \
                                      $RAWR_BUCKET $DATE_PREFIX

python batch-setup/make_meta_tiles.py --date-prefix $DATE_PREFIX \
                                      --missing-bucket $MISSING_BUCKET \
                                      $RAWR_BUCKET \
                                      $META_BUCKET \
                                      $DATE_PREFIX
```

Note that you will need to set several environment variables in the shell which executes this:

* `AWS_PROFILE` and `AWS_DEFAULT_REGION` to the profile your credentials are under and the region to set all this up in.
* `TILE_ASSET_BUCKET` and `TILE_ASSET_ROLE_ARN` to the bucket which stores your tile assets, and the role ARN to access it.
* `DB_PASSWORD` the database password. The database is isolated by security groups, but you should set something secure anyway.
* `RAWR_BUCKET`, `META_BUCKET` and `MISSING_BUCKET`, the buckets to store RAWR and meta tiles in, and the bucket to store the missing tile logs (used when retrying tiles which failed to generate).
* `DATE` the date of the planet file to download (`YYYY-MM-DD`).
* `PLANET_DATE` the same date as `YYMMDD`.
* `DATE_PREFIX` the prefix used on S3 buckets, conventionally the same as the planet date `YYYYMMDD`, but can be different if you want to do different runs.

## Importing the database

See [import](doc/import) for more information about Postgres setup and loading data. This can be useful if you don't want to do it manually, or need to troubleshoot the automated process.

## Building tiles

See [batch](doc/batch) for details about how to run RAWR and Metatile rendering via AWS Batch. This can be useful if you want to do it manually, or need to troubleshoot the automated process.

*On-Demand vs Spot Instances*

Currently, `batch_setup.py` creates On-Demand EC2 instances which are more expensive but easier to procure than Spot instances.
If you'd prefer to prioritize cost over overall build time, you can switch to Spot instances by reverting [this pr](https://github.com/tilezen/tileops/pull/86).
