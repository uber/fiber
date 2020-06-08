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
Local Fiber backend helps Fiber to run locally on a single machine.
This backend is mainly used for fast prototyping.
"""
import subprocess

import fiber.core as core
from fiber.core import ProcessStatus


class Backend(core.Backend):
    """
    We use subprocess to simulate job launch locally. Python's subprocess.Popen
    has already provided us all the necessary APIs to control subprocess, we
    just need to call them here.
    """
    name = "local"

    def __init__(self):
        pass

    def create_job(self, job_spec):
        proc = subprocess.Popen(job_spec.command)
        job = core.Job(proc, proc.pid)
        job.host = 'localhost'

        return job

    def get_job_status(self, job):
        proc = job.data

        if proc.poll() is not None:
            # subprocess has terminated
            return ProcessStatus.STOPPED

        return ProcessStatus.STARTED

    def wait_for_job(self, job, timeout):
        proc = job.data

        if timeout == 0:
            return proc.poll()

        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return None

        return proc.returncode

    def terminate_job(self, job):
        proc = job.data

        proc.terminate()

    def get_listen_addr(self):
        return "127.0.0.1", 0, "lo"
