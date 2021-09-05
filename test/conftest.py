# pylint: disable=redefined-outer-name,no-member
import json
import os
import subprocess
import time
from typing import Any

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

CONTAINER_NAME = "wal_rls_db_1"


@pytest.fixture(scope="session")
def dockerize_database():

    # Skip if we're using github actions CI
    if not "GITHUB_SHA" in os.environ:
        subprocess.call(["docker", "compose", "up", "-d"])
        # Wait for postgres to become healthy
        for _ in range(10):
            print(1)
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
        subprocess.call(["docker", "compose", "down", "-v"])
        return
    yield


@pytest.fixture(scope="session")
def engine(dockerize_database):
    eng = create_engine(f"postgresql://postgres:postgres@localhost:5432/postgres")
    yield eng
    eng.dispose()


@pytest.fixture(scope="function")
def sess(engine):

    conn = engine.connect()
    conn.execute(
        text("select * from pg_create_logical_replication_slot('rls_poc', 'wal2json')")
    )
    conn.execute(text("commit"))
    # Bind a session to the top level transaction
    _session = Session(bind=conn)
    yield _session
    # Close the session object
    _session.rollback()
    _session.close()

    # Cleanup between tests
    conn.execute(
        """
        select pg_drop_replication_slot('rls_poc');
    """
    )

    conn.execute(
        """
    drop schema public cascade;
    create schema public;
    """
    )
    conn.execute(
        """
    grant usage on schema public to postgres, anon, authenticated, service_role;
    alter default privileges in schema public grant all on tables to postgres, anon, authenticated, service_role;
    alter default privileges in schema public grant all on functions to postgres, anon, authenticated, service_role;
    alter default privileges in schema public grant all on sequences to postgres, anon, authenticated, service_role;
    truncate table cdc.subscription cascade;
    truncate table auth.users cascade;
    """
    )
    conn.execute(text("commit"))
    conn.close()


def pytest_addoption(parser: Any) -> None:
    parser.addoption(
        "--run-perf",
        action="store_true",
        default=False,
        help="run performance check",
    )


def pytest_collection_modifyitems(config: Any, items: Any) -> None:
    if not config.getoption("--run-perf"):
        skip = pytest.mark.skip(reason="performance test. Use --run-perf to run")
        for item in items:
            if "performance" in item.keywords:
                item.add_marker(skip)
    return
