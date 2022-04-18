FROM supabase/postgres:latest

RUN apt-get update
RUN apt install build-essential postgresql-server-dev-14 -y

RUN git clone https://github.com/eulerto/wal2json.git --depth 1 53b548a29ebd6119323b6eb2f6013d7c5fe807ec
RUN cd wal2json && make && make install
