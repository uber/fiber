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

import enum


MEM_CPU_RATIO = 2  # 2G per cpu


class ProcessStatus(enum.Enum):
    UNKNOWN = 0
    INITIAL = 1
    STARTED = 2
    STOPPED = 3


class JobSpec(object):
    def __init__(self, image=None, command=None, name=None, cpu=None, mem=None,
                 volumes=None, gpu=None):
        # Docker image used to launch this job
        self.image = image
        # Command to run in this job container, this should be a sequence
        # of program arguments instead of a single long string.
        self.command = command
        # Job name
        self.name = name
        # Maximum number of cpu cores this job can use
        self.cpu = cpu
        # Maximum number of cpu cores this job can use
        self.gpu = gpu
        # Maximum memory size in MB that this job can use
        #if mem is None:
        #    mem = cpu * MEM_CPU_RATIO
        self.mem = mem
        # volume name to be mounted, currently only used by k8s backend
        # For example:
        #     volumes = {
        #         "my_volume": {"mode": "rw", "bind": "/persistent"}
        #     }
        self.volumes = volumes

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return '<JobSpec: {}>'.format(vars(self))


class Job(object):
    # Data is used to hold backend specific data associated with this job
    data = None
    # Job id. This is set by backend and should only be used by Fiber backend
    jid = None
    # (Optional) The hostname/IP address for this job, this is used to
    # communicate with the master process. It is only used when
    # `ipc_admin_passive` is enabled.
    host = None

    def __init__(self, data, jid):
        self.data = data
        self.jid = jid

    def update(self):
        # update/refresh job attributes
        raise NotImplementedError


class Backend(object):
    @property
    def name(self):
        raise NotImplementedError

    def create_job(self, job_spec):
        """This function is called when Fiber wants to create a new Process."""
        raise NotImplementedError

    def get_job_status(self, job):
        """This function is called when Fiber wants to to get job status."""
        raise NotImplementedError

    def get_job_logs(self, job):
        """
        This function is called when Fiber wants to to get logs of this job
        """
        return ""

    def wait_for_job(self, job, timeout):
        """Wait for a specific job until timeout. If timeout is None,
        wait until job is done. Returns `None` if timed out or `exitcode`
        if job is finished.
        """
        raise NotImplementedError

    def terminate_job(self, job):
        """Terminate a job described by `job`."""
        raise NotImplementedError

    def get_listen_addr(self):
        """This function is called when Fiber wants to listen on a local
        address for incoming connection. It is currently used by Popen
        and Queue."""
        raise NotImplementedError
