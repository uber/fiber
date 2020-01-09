# Copyright 2020 Uber Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
import logging
from multiprocessing import reduction
import multiprocessing
import threading
import traceback
import select
import signal
import time


from fiber.init import init_fiber


logger = logging.getLogger('fiber')


def exit_by_signal():
    logger.info("Exiting, sending SIGTERM to current process")

    os.kill(os.getpid(), signal.SIGTERM)
    time.sleep(5)

    logger.warning("SIGTERM sent to current process but it's still running. "
                   "Trying os._exit()")
    os._exit(1)


def exit_on_fd_close(fd):
    while True:
        rl, _, _ = select.select([fd], [], [], 0)
        if fd in rl:
            logger.warning("Communication with master is closed, exiting")
            # sock is closed"
            exit_by_signal()
        time.sleep(1)


def spawn_prepare(fd):
    from_parent_r = os.fdopen(fd, "rb", closefd=False)
    preparation_data = reduction.pickle.load(from_parent_r)

    try:
        _config = preparation_data["fiber_config"]
        init_fiber(preparation_data["name"], **vars(_config))

        multiprocessing.spawn.prepare(preparation_data)
        self = reduction.pickle.load(from_parent_r)
        post_data = reduction.pickle.load(from_parent_r)

    except Exception as e:
        print("error", str(e))
        raise e

    # start watching thread so that when master quits, worker also quits
    td = threading.Thread(target=exit_on_fd_close, args=(fd,), daemon=True)
    logger.debug("Starting monitoring thread")
    td.start()

    self.ident = post_data["pid"]
    exitcode, err = self._bootstrap()
    if err:
        print("Exception in {}:".format(self))
        print(err)
        return exitcode

    return exitcode
