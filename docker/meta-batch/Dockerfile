FROM python:2

RUN apt-get -y update \
 && apt-get -y install libgeos-dev libboost-python-dev time \
 && rm -rf /var/lib/apt/lists/*

COPY raw_tiles /usr/src/raw_tiles
COPY tilequeue /usr/src/tilequeue
COPY vector-datasource /usr/src/vector-datasource

RUN mkdir -p /etc/tilequeue
COPY requirements.txt /etc/tilequeue/requirements.txt

RUN pip install -r /etc/tilequeue/requirements.txt \
                -e /usr/src/raw_tiles \
                -e /usr/src/tilequeue \
                -e /usr/src/vector-datasource

COPY logging.conf /etc/tilequeue/logging.conf
COPY config.yaml /etc/tilequeue/config.yaml

# NOTE: this also requires a command line argument --tile
# which is expected to come from the task definition
CMD [ "tilequeue", "meta-tile", "--config", "/etc/tilequeue/config.yaml"]
