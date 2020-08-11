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

"""
This module deals with Fiber configurations.

There are 3 way of setting Fiber configurations: config file, environment
variable and Python code. The priorities are: Python code > environment
variable > config file.

#### Config file

Fiber config file is a plain text file following Python's
[configparser](https://docs.python.org/3.6/library/configparser.html) file
format. It needs to be named `.fiberconfig` and put into the directory where
you launch your code.

An example `.fiberconfig` file:
```
[default]
log_level=debug
log_file=stdout
backend=local
```

#### Environment variable

Alternatively, you can also use environment variables to pass configurations to
Fiber. The environment variable names are in format `FIBER_` + config name in
upper case.

For example, an equivalent way of specifying the above config using environment
variables is:

```
FIBER_LOG_LEVEL=debug FIBER_LOG_FILE=stdout FIBER_BACKEND=local python code.py ...
```

#### Python code

You can also set Fiber config in your Python code:

```python
import fiber.config as fiber_config
...
def main():
    fiber_config.log_level = "debug"
    fiber_config.log_file = "stdout"
    fiber_config.backend = "local"
```

Note that almost all of the configurations needs to be set before you launch
any Fiber processes.
"""

import os
import logging
import configparser


_current_config = None
logger = logging.getLogger('fiber')


LOG_LEVELS = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
}

DEFAULT_IMAGE = "fiber-test:latest"


def str2bool(text):
    """Simple function to convert a range of values to True/False."""
    return text.lower() in ["true", "yes", "1"]


class Config(object):
    """Fiber configuration object. Available configurations:

    | key                   | Type | Default | Notes |
    | --------------------- |:------|:-----|:------|
    | debug                 | bool  | False | Set this to `True` to turn on debugging |
    | image                 | str   | None  | Docker image to use when starting  new processes |
    | default_image         | str   | None  | Default docker image to use when `image` config value  is not set |
    | backend               | str   | None  | Fiber backend to use when starting new processes. Check [here](platforms.md) for available backends |
    | default_backend       | str   | `local` | Default Fiber backend to use when `backend` config is not set |
    | log_level             | str/int | `logging.INFO` | Fiber log level. This config accepts either a int value (log levels from `logging` module like `logging.INFO`) or strings: `debug`, `info`, `warning`, `error`, `critical` |
    | log_file              | str   | `/tmp/fiber.log` | Default fiber log file path. Fiber will append the process name to this value and create one log file for each process. A special value `stdout` means to print the logs to standard output |
    | ipc_admin_master_port | int   | `0`   | The port that master process uses to communicate with child processes. Default value is `0` which means the master process will choose a random port |
    | kubernetes_namespace | str   | `default` | The namespace that Fiber `kubernetes` backend will use to create pods and do other work on Kubernetes |


    """
    def __init__(self, conf_file=None):
        # Not documented, people should not use this
        self.merge_output = False
        self.debug = False
        self.image = None
        self.default_image = DEFAULT_IMAGE
        self.backend = None
        self.default_backend = "local"
        # Not documented, this should be removed because it's not used for now
        self.use_bash = False
        self.log_level = logging.INFO
        self.log_file = "/tmp/fiber.log"
        # If ipc_active is True, Fiber worker processes will connect
        # to the master process. Otherwise, the master process will connect
        # to worker processes.
        # Not documented, should only be used internally
        self.ipc_active = True
        # if ipc_active is True, this can be 0, otherwise, it can only be a
        # valid TCP port number. Default 0.
        self.ipc_admin_master_port = 0
        # Not documented, this is only used when `ipc_active` is False
        self.ipc_admin_worker_port = 8000
        # Not documented, need to fine tune this
        self.cpu_per_job = 1
        # Not documented, need to fine tune this
        self.mem_per_job = None
        self.use_push_queue = True
        self.kubernetes_namespace = "default"

        if conf_file is None:
            conf_file = ".fiberconfig"

        # Load config from config file
        if os.path.exists(conf_file):
            logger.debug("loading config from %s", conf_file)
            config = configparser.ConfigParser()
            config.read(conf_file)
            for k in config["default"]:
                if k in self.__dict__:
                    self.__dict__[k] = config["default"][k]
                else:
                    raise ValueError(
                        'unknown config key "{}" in {}. Valid keys: '
                        '{}'.format(k, conf_file,
                                    [key for key in self.__dict__]))

        else:
            logger.debug("no fiber config file (%s) found", conf_file)

        # load environment variable overwrites
        for k in self.__dict__:
            name = "FIBER_" + k.upper()
            val = os.environ.get(name, None)
            if val:
                self.__dict__[k] = val

        # rewrite values
        if isinstance(self.log_level, str):
            level = self.log_level.lower()
            if level not in LOG_LEVELS:
                logger.debug("bad logging level: %s", self.log_level)
                level = logging.NOTSET
            else:
                level = LOG_LEVELS[level]
            self.log_level = level

        if isinstance(self.ipc_active, str):
            self.ipc_active = str2bool(self.ipc_active)

        if isinstance(self.cpu_per_job, str):
            self.cpu_per_job = int(self.cpu_per_job)

        if isinstance(self.mem_per_job, str):
            self.mem_per_job = int(self.mem_per_job)

    def __repr__(self):
        return repr(self.__dict__)

    @classmethod
    def from_dict(cls, kv):
        obj = cls()
        for k in kv:
            setattr(obj, k, kv[k])

        return obj


def get_object():
    """
    Get a Config object representing current Fiber config

    :returns: a Config object
    """
    # When an config object is needed, call this method to get an
    # concrete Fiber config object

    global _current_config

    return Config.from_dict(get_dict())


def get_dict():
    """
    Get current Fiber config in a dictionary

    :returns: a Python dictionary with all the current Fiber configurations
    """
    global_vars = globals()

    return {k: global_vars[k] for k in vars(_current_config)}


def init(**kwargs):
    """
    Init Fiber system and set config values.

    :param kwargs: If kwargs is not None, init Fiber system with corresponding
        key/value pairs in kwargs as config keys and values.

    :returns: A list of config keys that was updated in this function call.
    """
    _config = Config()

    # handle overwrites
    _config.__dict__.update(kwargs)

    updates = []
    global_vars = globals()

    for k in vars(_config):
        # fine diffs and write them
        val = getattr(_config, k)
        if k not in global_vars or global_vars[k] != val:
            global_vars[k] = val
            updates.append(k)

    global_vars["_current_config"] = _config

    logger.debug("Inited fiber with config: %s", vars(_config))

    return updates
