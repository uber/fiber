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
Kubernetes backend. Helps Fiber to launch jobs on Kubernetes.
"""

import logging
import multiprocessing as mp
import socket
import time
import uuid

import psutil
from kubernetes import client, config
from kubernetes.client.rest import ApiException

import fiber
import fiber.core as core
import fiber.config as fiber_config
from fiber.core import ProcessStatus
from fiber.util import find_ip_by_net_interface, find_listen_address


logger = logging.getLogger("fiber")


PHASE_STATUS_MAP = {
    "Pending": ProcessStatus.INITIAL,
    "Running": ProcessStatus.STARTED,
    "Succeeded": ProcessStatus.STOPPED,
    "Failed": ProcessStatus.STOPPED,
    "Unknown": ProcessStatus.STOPPED,
}


class Backend(core.Backend):
    name = "kubernetes"

    def __init__(self, incluster=True):
        if incluster:
            config.load_incluster_config()
        else:
            config.load_kube_config()
        self.config = client.Configuration()
        self.batch_api = client.BatchV1Api(client.ApiClient(self.config))
        self.core_api = client.CoreV1Api(client.ApiClient(self.config))
        self.default_namespace = fiber_config.kubernetes_namespace

        if incluster:
            podname = socket.gethostname()
            pod = self.core_api.read_namespaced_pod(podname,
                                                    self.default_namespace)
            # Current model assume that Fiber only lauches 1 container per pod
            self.current_image = pod.spec.containers[0].image
            self.volumes = pod.spec.volumes
            self.mounts = pod.spec.containers[0].volume_mounts

            #if pod.spec.volumes:
            #    self.current_mount = pod.spec.volumes[0].persistent_volume_claim.claim_name
            #else:
            #    self.current_mount = None
        else:
            self.current_image = None
            self.mounts = None
            self.volumes = None

    def _get_resource_requirements(self, job_spec):
        #requests = {}
        limits = {}

        if job_spec.cpu:
            limits["cpu"] = str(job_spec.cpu)
        else:
            # by default, allocate 1 cpu
            limits["cpu"] = str(1)

        if job_spec.mem:
            limits["memory"] = str(job_spec.mem) + "Mi"

        if job_spec.gpu:
            limits["nvidia.com/gpu"] = str(job_spec.gpu)

        if limits != {}:
            rr = client.V1ResourceRequirements()
            rr.limits = limits
            return rr

        return None


    def create_job(self, job_spec):
        logger.debug("[k8s]create_job: %s", job_spec)
        body = client.V1Pod()
        name = "{}-{}".format(
            job_spec.name.replace("_", "-").lower(),
            str(uuid.uuid4())[:8]
        )
        body.metadata = client.V1ObjectMeta(
            namespace=self.default_namespace, name=name
        )

        # set environment varialbes
        # TODO(jiale) add environment variables

        image = job_spec.image if job_spec.image else self.current_image

        container = client.V1Container(
            name=name, image=image, command=job_spec.command, env=[],
            stdin=True, tty=True,
        )

        rr = self._get_resource_requirements(job_spec)
        if rr:
            logger.debug(
                "[k8s]create_job, container resource requirements: %s",
                job_spec
            )
            container.resources = rr

        body.spec = client.V1PodSpec(
            containers=[container],
            restart_policy="Never"
        )

        # propagate mount points to new containers if necesary
        if job_spec.volumes:
            volumes = []
            volume_mounts = []

            for pd_name, mount_info in job_spec.volumes.items():
            #volume_name = job_spec.volume if job_spec.volume else self.current_mount
                pvc = client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=pd_name
                )
                volume = client.V1Volume(
                    persistent_volume_claim=pvc,
                    name="volume-" + pd_name,
                )
                volumes.append(volume)
                mount = client.V1VolumeMount(
                    mount_path=mount_info["bind"],
                    name=volume.name,
                )
                if mount_info["mode"] == "r":
                    mount.read_only = True
                volume_mounts.append(mount)
            body.spec.volumes = volumes
            container.volume_mounts = volume_mounts
        elif self.mounts:
            body.spec.volumes = self.volumes
            container.volume_mounts = self.mounts

        logger.debug("[k8s]calling create_namespaced_pod: %s", body.metadata.name)
        try:
            v1pod = self.core_api.create_namespaced_pod(
                self.default_namespace, body
            )
        except ApiException as e:
            raise e

        return core.Job(v1pod, v1pod.metadata.uid)

    def get_job_status(self, job):
        v1pod = job.data
        name = v1pod.metadata.name
        namespace = v1pod.metadata.namespace

        try:
            v1pod = self.core_api.read_namespaced_pod_status(name, namespace)
            logger.debug("get_job_status: v1pod: %s", v1pod.status.phase)
        except ApiException as e:
            logger.debug(
                "[k8s] Exception when calling CoreApi->"
                "read_namespaced_pod_status: %s",
                str(e),
            )
            return ProcessStatus.STOPPED

        container_statuses = v1pod.status.container_statuses
        if container_statuses:
            # container_statuses[0].state.terminated is updated faster than pod_status.phase
            if container_statuses[0].state.terminated:
                return ProcessStatus.STOPPED
        pod_status = v1pod.status
        return PHASE_STATUS_MAP[pod_status.phase]

    def get_job_logs(self, job):
        v1job = job.data
        name = v1job.metadata.name
        namespace = v1job.metadata.namespace

        try:
            logs = self.batch_api.read_namespaced_pod_log(name, namespace)
        except ApiException as e:
            logger.debug(
                "[k8s] Exception when calling BatchV1Api->"
                "read_namespaced_job_status: %s",
                str(e),
            )
            raise e

        return logs

    def wait_for_job(self, job, timeout):
        logger.debug("[k8s]wait_for_job timeout=%s", timeout)

        total = 0
        while self.get_job_status(job) != ProcessStatus.STOPPED:
            time.sleep(1)
            total += 1
            if timeout is None:
                # timeout is None means wait until process exits
                continue
            if total > timeout:
                logger.debug("[k8s]wait_for_job done(timeout)")
                return None

        v1pod = job.data
        name = v1pod.metadata.name
        namespace = v1pod.metadata.namespace
        try:
            v1pod = self.core_api.read_namespaced_pod_status(name, namespace)
        except ApiException as e:
            logger.debug(
                "[k8s] Exception when calling CoreApi->"
                "read_namespaced_pod_status: %s",
                str(e),
            )
            # job has been deleted
            return -1

        pod_status = v1pod.status

        terminated = pod_status.container_statuses[0].state.terminated
        if terminated is None:
            logger.debug("[k8s]wait_for_job done: container is not terminated")
            return None

        logger.debug("[k8s]wait_for_job done: container terminated with "
                     "code: {}".format(terminated.exit_code))
        return terminated.exit_code

    def terminate_job(self, job):
        v1job = job.data
        name = v1job.metadata.name
        namespace = v1job.metadata.namespace
        grace_period_seconds = 60
        body = client.V1DeleteOptions()

        try:
            logger.debug(
                "calling delete_namespaced_pod(%s, %s, grace_period_seconds=%s)",
                name, namespace, grace_period_seconds,
            )
            self.core_api.delete_namespaced_pod(
                name, namespace, grace_period_seconds=grace_period_seconds,
                body=body,
            )
        except ApiException as e:
            logger.debug(
                "[k8s] Exception when calling " "delete_namespaced_pod: %s",
                str(e),
            )
            raise e

    def get_listen_addr(self):
        ip = None

        # if fiber.current_process() is multiprocessing.current_process():
        if not isinstance(fiber.current_process(), fiber.Process):
            # not inside docker
            # logger.debug("use interface docker0")
            ifce = "eth0"
            ip = find_ip_by_net_interface(ifce)
        else:
            # inside a Fiber process
            # logger.debug("use interface eth0")
            # ip = find_ip_by_net_interface("eth0")
            ip, ifce = find_listen_address()

        # ip = find_ip_by_net_interface(ifce)
        if ip is None:
            raise mp.ProcessError(
                "Can't find a usable IPv4 address to listen. ifce_name: {}, "
                "ifces: {}".format(ifce, psutil.net_if_addrs())
            )
        # use 0 to bind to a random free port number
        return ip, 0, ifce
