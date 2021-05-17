AWS Batch
=========

**NOTE: The following information is about what goes on under the hood of the automated scripts (`batch-setup/make_*_tiles.py`).**

AWS Batch is used to organize the steps to generate tiles. There are 3 types of jobs that run:

1. RAWR tile

2. Meta tile high zooms

3. Meta tile low zooms

Batch Runs
----------

### RAWR

These jobs generate RAWR tiles. They read from RDS instances, and write tiles to S3.

Jobs should be enqueued at zoom 7.

### Meta tile high zooms

These jobs generate metatiles for high zooms, `[z10,...]`. They read from the RAWR tiles, and generate metatiles on S3.

### Meta tile low zooms

These jobs generate metatiles for low zooms, `[z0,z10)`. They read from postgresql, and generate metatiles on S3.

Docker Images
-------------

### Building

Under docker/ each directory contains a make file which takes care of generating the image. The default `image` target builds the image, and the `push` target pushes it over to AWS ECR.

First, the repository for the image should be created in AWS ECR. Then, in the Makefile, the `IMAGE` variable should match that. Additionally, each account will have its own registry url, which can be found in ECR. This needs to match the Makefile too.

Submitting Jobs
---------------

With the images in place, jobs can now be submitted to AWS Batch.

*NOTE*: this expects that batch has been already set up. If not, a compute environment, job queue, and necessary iam roles will need to be set up first before jobs can be submitted.

### Job Definitions

Job definitions will need to be created for each batch run. The `tz-batch-create-job-definition` command can help here. It takes a [yaml file](../../go/yml/batch-job-definitions.yaml.sample) as input. Multiple job defintions can be created simultaneously.

It should be sufficient to set the vcpus to 1. Memory (specified in megs) should vary based on the job, and is currently being dialed in, but a reasonable starting place should be:

* rawr -> 8192
* meta high zoom -> 4096
* meta low zoom -> 2048

The command should be the corresponding tilequeue command, and it should have the `tile` and `run_id` specified as parameters, eg: `["--tile", "Ref::tile"]` and `["--run_id", "Ref::run_id"]`. These parameters will be specified when the jobs get enqueued.

Each job definition needs to have the appropriate environment variables configured. Currently this is:

#### Rawr

* `TILEQUEUE__rawr__postgresql__host`
* `TILEQUEUE__rawr__postgresql__dbname`
* `TILEQUEUE__rawr__postgresql__user`
* `TILEQUEUE__rawr__postgresql__password`
* `TILEQUEUE__rawr__sink__bucket`
* `TILEQUEUE__rawr__sink__region`
* `TILEQUEUE__rawr__sink__prefix`

#### Meta high zoom

* `TILEQUEUE__store__name`
* `TILEQUEUE__store__date-prefix`
* `TILEQUEUE__rawr__source__s3__bucket`
* `TILEQUEUE__rawr__source__s3__region`
* `TILEQUEUE__rawr__source__s3__prefix`

#### Meta low zoom

* `TILEQUEUE__postgresql__host`
* `TILEQUEUE__postgresql__dbnames`
* `TILEQUEUE__postgresql__user`
* `TILEQUEUE__postgresql__password`
* `TILEQUEUE__store__name`
* `TILEQUEUE__store__date-prefix`

*NOTE*: the values for each of these should be strings. Furthermore, the string itself will be interpolated by tilequeue as yaml. For example:

    TILEQUEUE__postgresql__dbnames: "[gis]"

### Submitting

The `tilequeue batch-enqueue` command allows job submission. This can be run locally, and submitting most jobs takes about 20 minutes.

When running the enqueue, update the configuration file to contain the appropriate values for the batch section. 

* `job-definition`
* `job-queue`
* `job-name-prefix`
* `run_id`

*NOTE*: when the `tilequeue` command executes, it will pick up the `run-id` and emit it with every log entry. This can be helpful to select all log messages for a particular batch run, as all log entries are placed in the same batch log group. It's recommended to set this to a datestamp with a description for the run attached to it, eg "rawr-20180403".

#### rawr

    tilequeue batch-enqueue --config config.yaml

#### meta high zoom

    tilequeue batch-enqueue --config config.yaml

#### meta low zoom

    tilequeue batch-enqueue --config config.yaml --pyramid

*NOTE*: Jobs will be enqueued at the batch `queue-zoom` configuration value. This is expected to be 7.
The `pyramid` option to `batch-enqueue` will additionally enqueue all tiles lower than the `queue-zoom`, which is required for low zoom tiles. Assuming a `queue-zoom` of 7, the jobs enqueued will be zooms `[0, 7]` with `--pyramid`.
Furthermore, it also takes `--tile` and `--file` arguments. `--tile` will only enqueue a single tile, and `--file` will enqueue all tiles in a particular file. These are useful for initially testing a single tile to ensure that batch is set up correctly, and for iterating on enqueueing additional tiles to reprocess.


# Rebuild
After each build is finished, to rebuild the tiles that intersects with a bounding box, you can logon to the tiles ops runner EC2 instance, and trigger a command similar to the following

```
BBOX_WEST=-123.571730 BBOX_SOUTH=45.263862 BBOX_EAST=-118.386183 BBOX_NORTH=48.760348 /usr/bin/nohup /usr/local/bin/bbox_rebuild.sh &
```

(the above command will rebuild all the rawr and meta tiles that wraps the bounding box BBOX_WEST=-123.571730 BBOX_SOUTH=45.263862 BBOX_EAST=-118.386183 BBOX_NORTH=48.760348)