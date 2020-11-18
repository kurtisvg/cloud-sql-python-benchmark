#!/usr/bin/env python

# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import logging
import os
import time
import random
from collections.abc import Coroutine

import asyncpg

logger = logging.getLogger()

# configuration for the database
db_user: str = os.environ["DB_USER"]  # e.g. "my-db-user"
db_pass: str = os.environ["DB_PASS"]  # e.g. "my-db-password"
db_name: str = os.environ["DB_NAME"]  # e.g. "my-database"
db_host: str = os.environ.get("DB_HOST")  # e.g. 127.0.0.1:5432
db_socket: str = os.environ.get("DB_SOCKET")  # e.g.


# intialize the pool to the databse
def init_pool():
    db_opts: Mapping = {
        "user": db_user,
        "password": db_pass,
        "database": db_name,
        "min_size": 0,
        "max_size": 10,
    }

    if db_host:
        # Extract host and port from db_host
        host_args = db_host.split(":")
        db_opts["host"] = host_args[0]
        db_opts["port"] = int(host_args[1])
    elif db_socket:
        db_opts["host"] = db_socket
    else:
        logger.fatal("db_host or db_dir not set")
        raise Exception("db_host or db_dir must be set")

    return asyncpg.create_pool(**db_opts)


# schedules a function to be called at a fixed rate
async def schedule_fixed_rate(sec: int, func: Coroutine):
    while True:
        await asyncio.sleep(sec)
        asyncio.create_task(func())


# tests connection to the server and logs the time to complete
async def connect_with_pool(pool: asyncpg.pool.Pool):
    now = time.monotonic()
    connStart, connEnd = now, now
    tranStart, tranEnd = 0, 0
    try:
        async with pool.acquire() as conn:
            now = time.monotonic()
            tranStart, tranEnd = now, now
            try:
                await conn.execute("SELECT 1;", timeout=10)
            finally:
                tranEnd = time.monotonic()
    except Exception:
        logger.exception(f"Connection failed!")
    finally:
        connEnd = time.monotonic()
    connDiff, tranDiff = connEnd - connStart, tranEnd - tranStart

    logger.info(
        f" connect complete: conn={connDiff*100:.2f}ms, trans={tranDiff*100:.2f}ms, diff={(connDiff-tranDiff)*100:.2f}ms"
    )


async def main():
    async with init_pool() as pool:
        await schedule_fixed_rate(.5, lambda: connect_with_pool(pool))

    logger.info("Hello world!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.get_event_loop().run_until_complete(main())
