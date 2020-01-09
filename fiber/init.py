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


import logging
import os
import multiprocessing as mp

import fiber.process
import fiber.config as fiber_config
import fiber.backend as fiber_backend


def init_logger(config, proc_name=None):
    logger = logging.getLogger("fiber")
    if config.log_file.lower() == "stdout":
        handler = logging.StreamHandler()
    else:
        log_dir = os.path.dirname(config.log_file)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        if not proc_name:
            p = fiber.process.current_process()
            proc_name = p.name

        log_file = config.log_file + '.' + proc_name
        handler = logging.FileHandler(log_file, mode="w")

    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s:%(processName)s(%(process)d):'
        '%(threadName)s(%(thread)d){%(filename)s:%(lineno)d} %(message)s')
    handler.setFormatter(formatter)
    if logger.handlers is None or len(logger.handlers) == 0:
        logger.addHandler(handler)
    else:
        logger.handlers[-1] = handler
    logger.propagate = False


def init_fiber(proc_name=None, **kwargs):
    """
    Initialize Fiber. This function is called when you want to re-initialize
    Fiber with new config values and also re-init loggers.

    :param proc_name: process name of current process
    :param kwargs: If kwargs is not None, init Fiber system with corresponding
        key/value pairs in kwargs as config keys and values.
    """
    # check backend
    if "backend" in kwargs:
        backend = kwargs["backend"]
        if (backend not in fiber_backend.available_backend
           and backend is not None):

            raise mp.ProcessError("Invalid backend: {}".format(backend))

    updates = fiber_config.init(**kwargs)

    if "log_level" in updates or "log_file" in updates:
        _config = fiber_config.get_object()
        init_logger(_config, proc_name=proc_name)
