# Importing the database

**NOTE: This should all be handled automatically by the `import/import.py` script. The following information is about what it's doing under the hood.**

This is a set of scripts to perform a database import using the `vector-datasource` setup procedures. It performs a series of steps:

1. Creating a new RDS database.
2. Creating an EC2 instance to run `osm2pgsql`.
3. Downloading and importing the latest OSM planet file.
4. Downloading and importing extra data from Natural Earth and static Tilezen data.
5. Performing extra updates to add `min_zoom` and indexes.

The input to this is the latest planet and `vector-datasource` code, and the end result should be an RDS database snapshot suitable for loading and Batch-generating RAWR tiles.

## Setup

### Local setup

First, make sure you have a Python environment set up. It can be useful to isolate your environment on a per-project basis by using [`virtualenv`](https://virtualenv.pypa.io/en/stable/) or [`pyenv`](https://github.com/pyenv/pyenv). Within the environment you want to use, install the requirements:

```
pip install -Ur requirements.txt
```

### AWS environment setup

**NOTE:** This is only necessary the first time you run the import, or if you want to isolate different imports from each other. If not, it should be safe to re-use the same S3 bucket and IAM instance profile.

An S3 bucket to store the `flat.nodes` file must be set up before the import run. The `flat.nodes` file is technically not necessary to generate RAWR tiles, and is only used when updating a database with streaming updates from OSM. However, it seems like something we'll need or want in the future, so it's left mandatory for now. Note that the `flat.nodes` file is uploaded with a date prefix, so is kept separate from previous and future imports.

An IAM instance profile with the ability to read and write the S3 bucket containing the `flat.nodes` file.

## Running the import

When running the import, it is necessary to supply a database password. This will be required to access the database, on top of the necessary AWS Security Groups. Given the security provided by the SGs, the password theoretically could be unsafe, but it's probably not a good idea to leave it blank. If in doubt, generate a strong password and store it in an approved password vault.

The import runs in two separate parts:

1. The import script (`import.py`), which controls the import process, but doesn't directly do any importing. It is intended to be run from a local machine, not from an EC2 instance. It might not work from within EC2, or you might need to set up additional permissions for the instance profile on the machine running the import.
2. A "remote" set of scripts which run on an EC2 instance created specifically to run `osm2pgsql` by `import.py`. This requires quite a lot of RAM and fast disk for the "flat nodes" file. Several other processes also run on the EC2 instance to download data and import it into the database.

```
python import.py s3-bucket s3-bucket-region iam-instance-profile database-password
```

The import is intended to be idempotent, and should re-use existing resources (i.e: database, EC2 instances) where possible. This means that it isn't necessary for the computer launching the import to stay online throughout the whole process - at worst, the import will stall if the computer goes offline. It should not fail for that reason alone. A completed import is safe to re-run, and will halt if the output database snapshot is already present.

Eventually, the import should either succeed or fail, and stop in either one of those states. Re-running after that should either result in the message that it has failed and needs manual intervention or has already finished and will not re-run without manual intervention.

During the longest part of the import process, running `osm2pgsql`, the import script should poll the current status and display it to the screen. For more information, you can log into the EC2 instance running `osm2pgsql` and `tail nohup.log`.
