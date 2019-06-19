FROM python:2

RUN apt-get -y update \
 && apt-get -y install libgeos-dev libboost-python-dev libgdal-dev \
 && rm -rf /var/lib/apt/lists/*

COPY raw_tiles /usr/src/raw_tiles
COPY tilequeue /usr/src/tilequeue
COPY vector-datasource /usr/src/vector-datasource

RUN mkdir -p /etc/tilequeue
COPY requirements.txt /etc/tilequeue/requirements.txt

# sadly, the version of GDAL available from Debian Jessie has to match the version
# installed in Pip, and this requires these environment variables to be set in order
# to find the GDAL header files. this seems to have been fixed in more recent versions
# of the GDAL Pip package, so could be removed if the python:2 base image upgrades
# from Jessie.
RUN export CPLUS_INCLUDE_PATH=/usr/include/gdal \
 && export C_INCLUDE_PATH=/usr/include/gdal \
 && pip install -r /etc/tilequeue/requirements.txt \
                -e /usr/src/raw_tiles \
                -e /usr/src/tilequeue \
                -e /usr/src/vector-datasource \
 && unset CPLUS_INCLUDE_PATH \
 && unset C_INCLUDE_PATH

COPY logging.conf /etc/tilequeue/logging.conf
COPY config.yaml /etc/tilequeue/config.yaml

# NOTE: this also requires a command line argument --tile
# which is expected to come from the task definition
CMD [ "tilequeue", "meta-tile", "--config", "/etc/tilequeue/config.yaml"]
