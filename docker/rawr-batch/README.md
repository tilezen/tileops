# RAWR tile generation

The idea here is to make a Docker image which can be run in AWS Batch.

The `Makefile` is an awful way to do it, so apologies for that. However, checking out the code externally from Docker and `COPY`ing it in sidesteps the difficulty of injecting credentials into the Docker image to check the code out without embedding them in the `Dockerfile`.

If you (manually) shift the branch of the `raw_tiles` and `tilequeue` check-outs to the latest branches I pushed (`mamos/random-database-connection` and `mamos/rawr-tile-command` respectively), then it should be possible to run the Docker image, e.g:

```
docker run --env-file example.env.list e02c9dfde60a \
  tilequeue rawr-tile --config /usr/src/tilequeue/config.yaml \
  --tile 10/0/0
```

IIRC, it's possible to affect both the environment and command line for each Batch job, so we should be able to use this to run the RAWR tile generation.
