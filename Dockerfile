FROM postgres:13

# install wal2json
RUN apt-get update \
    && apt-get install postgresql-$PG_MAJOR-wal2json -y

CMD ["postgres", "-c", "wal_level=logical", "-c", "fsync=off"]
#, "-c", "include-pk=true"]
