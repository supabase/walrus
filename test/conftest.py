# pylint: disable=redefined-outer-name,no-member
import json
import os
import subprocess
from pathlib import Path
import time
from flupy import walk_files

import pytest
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

CONTAINER_NAME = "wal_rls_dev"
IMAGE_NAME = "pg_wal_rls"
DB_NAME = "gqldb"
PORT = 5403


@pytest.fixture(scope="session")
def dockerize_database():

    # Skip if we're using github actions CI
    if not "GITHUB_SHA" in os.environ:
        subprocess.call(
            [
                "docker",
                "run",
                "--rm",
                "--name",
                CONTAINER_NAME,
                "-p",
                f"{PORT}:5432",
                "-d",
                "-e",
                f"POSTGRES_DB={DB_NAME}",
                "-e",
                "POSTGRES_PASSWORD=password",
                "-e",
                "POSTGRES_USER=postgres",
                "--health-cmd",
                "pg_isready",
                "--health-interval",
                "3s",
                "--health-timeout",
                "3s",
                "--health-retries",
                "15",
                IMAGE_NAME
            ]
        )
        # Wait for postgres to become healthy
        for _ in range(10):
            out = subprocess.check_output(["docker", "inspect", CONTAINER_NAME])
            container_info = json.loads(out)
            container_health_status = container_info[0]["State"]["Health"]["Status"]
            if container_health_status == "healthy":
                break
            else:
                time.sleep(1)
        else:
            raise Exception("Container never became healthy")
        yield
        subprocess.call(["docker", "stop", CONTAINER_NAME])
        return
    yield


@pytest.fixture(scope="session")
def engine(dockerize_database):
    eng = create_engine(f"postgresql://postgres:password@localhost:{PORT}/{DB_NAME}")

    for str_path in walk_files('sql'):
        path = Path(str_path)
        contents = path.read_text()
        with eng.connect() as conn:
            conn.execute(text(contents))
            conn.execute(text("commit"))


    yield eng
    eng.dispose()


@pytest.fixture(scope="function")
def sess(engine):

    conn = engine.connect()
    conn.execute(text("select * from pg_create_logical_replication_slot('rls_poc', 'wal2json')"))
    conn.execute(text("commit"))
    # Bind a session to the top level transaction
    _session = Session(bind=conn)
    yield _session
    # Close the session object
    _session.rollback()
    _session.close()

    # Cleanup between tests
    conn.execute("""
        select pg_drop_replication_slot('rls_poc');
    """)

    conn.execute("drop schema public cascade;")
    conn.execute("create schema public;")
    conn.close()
