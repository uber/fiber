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
import multiprocessing as mp
import os
import sys
import threading
import time
import uuid
from os.path import expanduser

import docker
import psutil
import requests

import fiber
import fiber.core as core
import fiber.config as config
from fiber.core import ProcessStatus
from fiber.util import find_ip_by_net_interface, find_listen_address

logger = logging.getLogger('fiber')


STATUS_MAP = {
    "restarting": ProcessStatus.INITIAL,
    "created": ProcessStatus.INITIAL,
    "running": ProcessStatus.STARTED,
    "paused": ProcessStatus.STARTED,
    "exited": ProcessStatus.STOPPED,
}

HOME_DIR = expanduser("~")


class DockerJob(core.Job):
    def update(self):
        container = self.data
        self.host = container.attrs['NetworkSettings']['IPAddress']


class Backend(core.Backend):
    name = "docker"

    def __init__(self):
        # Based on this link, no lock is needed accessing self.client
        # https://github.com/docker/docker-py/issues/619
        self.client = docker.from_env()

    def create_job(self, job_spec):
        logger.debug("[docker]create_job: %s", job_spec)
        cwd = os.getcwd()
        volumes = {cwd: {'bind': cwd, 'mode': 'rw'},
                   HOME_DIR: {'bind': HOME_DIR, 'mode': 'rw'}}
        debug = config.debug

        tty = debug
        stdin_open = debug

        if job_spec.image is None:
            image = config.default_image
        else:
            image = job_spec.image

        try:
            container = self.client.containers.run(
                image,
                job_spec.command,
                name=job_spec.name + '-' + str(uuid.uuid4()),
                volumes=volumes,
                cap_add=["SYS_PTRACE"],
                tty=tty,
                stdin_open=stdin_open,
                detach=True
            )
        except docker.errors.ImageNotFound:
            raise mp.ProcessError(
                "Docker image \"{}\" not found or cannot be pulled from "
                "docker registry.".format(job_spec.image))

        except docker.errors.APIError as e:
            raise mp.ProcessError(
                    "Failed to start docker container: {}".format(e)
            )

        job = DockerJob(container, container.id)
        # delayed
        container._fiber_backend_reloading = False
        return job

    def _reload(self, container):
        container._fiber_backend_reloading = True
        logger.debug("container reloading %s", container.name)
        container.reload()
        if config.debug:
            time.sleep(0.1)
        else:
            time.sleep(1)

        container._fiber_backend_reloading = False

    def get_job_logs(self, job):
        container = job.data
        return container.logs(stream=False).decode('utf-8')

    def get_job_status(self, job):
        container = job.data

        if config.merge_output:
            print(container.logs(stream=False).decode('utf-8'))

        logging.debug("get_job_status: %s", container.status)
        status = STATUS_MAP[container.status]

        if not container._fiber_backend_reloading:
            td = threading.Thread(target=self._reload, args=(container,))
            td.start()
            logger.debug("start container reloading thread %s", container.name)
        return status

    def wait_for_job(self, job, timeout):
        container = job.data
        logger.debug("wait_for_job: %s", container.name)

        if config.merge_output:
            print(container.logs(stream=False).decode('utf-8'))

        # Can't set timeout to 0 for container.wait. Use a very
        # short wait time instead
        if timeout is not None and timeout <= 0:
            timeout = 0.05
        try:
            res = container.wait(timeout=timeout)
        except requests.exceptions.ConnectionError:
            # Docker SDK for Python document said that wait will raise
            # requests.exceptions.ReadTimeout exception, but actually it
            # raises requests.exceptions.ConnectionError.
            # See https://docker-py.readthedocs.io/en/stable/containers.html#docker.models.containers.Container.wait # noqa: E501

            logger.debug("wait_for_job done(ConnectionError): %s",
                         container.name)
            return None
        except requests.exceptions.ReadTimeout:
            # Incase it raises ReadTimeout
            logger.debug("wait_for_job done(timeout): %s", container.name)
            return None

        if config.merge_output:
            print(container.logs(stream=False).decode('utf-8'))

        logger.debug("wait_for_job done: %s", container.name)

        return res['StatusCode']

    def terminate_job(self, job):
        logging.debug("terminate_job")
        container = job.data

        if config.merge_output:
            print(container.logs(stream=False).decode('utf-8'))

        logger.debug("before terminating a container, status: %s",
                     container.status)
        try:
            container.kill()
        except docker.errors.APIError as e:
            if e.status_code == 409:
                logger.warning("container is not running %s", str(e))
                # Ignore 409 error which usually means container is not running
                return
            raise e
        logger.debug("terminate job finished, %s", container.status)

    def get_listen_addr(self):
        ip, ifce = find_listen_address()

        if sys.platform == "darwin":
            # use the same hostname for both master and non master process
            # because docker.for.mac.localhost resolves to different inside
            # and outside docker container. "docker.for.mac.localhost" is
            # the name that doesn't change in and outside the container.
            return "docker.for.mac.localhost", 0, ifce

        if not isinstance(fiber.current_process(), fiber.Process):
            # not inside docker
            ifce = "docker0"
            ip = find_ip_by_net_interface(ifce)

        if ip is None:
            raise mp.ProcessError(
                "Can't find a usable IPv4 address to listen. ifce_name: {}, "
                "ifces: {}".format(ifce, psutil.net_if_addrs()))
        # use 0 to bind to a random free port number
        return ip, 0, ifce
