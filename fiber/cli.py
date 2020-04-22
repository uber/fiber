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
fiber.cli

This module contains functions for the `fiber` command line tool. `fiber`
command line tool can be use do to mange the workflow of running jobs on a
computer cluster.

Check [here](getting-started.md#running-with-fiber-command) for an example of
how to use this command.

"""

import os
import json
import subprocess as sp
from pathlib import Path

import click

import fiber
import fiber.core as core
from fiber.core import ProcessStatus


CONFIG = {}


def get_backend(platform):
    from fiber.kubernetes_backend import Backend as K8sBackend
    backend = K8sBackend(incluster=False)
    return backend


def find_docker_files():
    """Find all possible docker files on current directory."""
    p = Path(".")
    q = p / "Dockerfile"

    files = list(p.glob("*.docker"))

    if q.exists():
        files.append(q)

    return files


def select_docker_file(files):
    """Ask user which docker file to use and return a PurePath object."""
    num = 0
    n = len(files)

    if n > 1:
        print("Available docker files:")
        for i, f in enumerate(files):
            print(i + 1, f.name)

        while True:
            end = len(files)
            input_str = input("which docker file to use? [1-{}] ".format(end))

            try:
                num = int(input_str) - 1
                if num < 0 or num >= end:
                    raise ValueError

                break
            except (TypeError, ValueError):
                print(
                    "Invalid input: {}. Please choose from [1-{}]".format(
                        input_str, end
                    )
                )
                continue

    return files[num]


def get_default_project_gcp():
    """Get default GCP project name."""
    name = sp.check_output(
        "gcloud config list --format 'value(core.project)' 2>/dev/null",
        shell=True,
    )
    return name.decode("utf-8").strip()


def parse_file_path(path):
    parts = path.split(":")
    if len(parts) == 1:
        return (None, path)

    if len(parts) > 2:
        raise ValueError("Bad path: {}".format(path))

    return (parts[0], parts[1])


@click.command()
@click.argument("src")
@click.argument("dst")
def cp(src, dst):
    """Copy file from a persistent storage"""
    platform = CONFIG["platform"]

    parts_src = parse_file_path(src)
    parts_dst = parse_file_path(dst)

    if parts_src[0] and parts_dst[0]:
        raise ValueError(
            "Can't copy from persistent storage to persistent storage"
        )

    if parts_src[0]:
        volume = parts_src[0]
    elif parts_dst[0]:
        volume = parts_dst[0]
    else:
        raise ValueError("Must copy/to from a persistent volume")

    k8s_backend = get_backend(platform)

    job_spec = core.JobSpec(
        image="alpine:3.10",
        name="fiber-cp",
        command=["sleep", "60"],
        volumes={volume: {"mode": "rw", "bind": "/persistent"}},
    )
    job = k8s_backend.create_job(job_spec)
    pod_name = job.data.metadata.name

    print("launched pod: {}".format(pod_name))
    exitcode = os.system(
        "kubectl wait --for=condition=Ready pod/{}".format(pod_name)
    )

    """
    status = k8s_backend.get_job_status(job)
    while status == ProcessStatus.INITIAL:
        print("Waiting for pod {} to be up".format(pod_name))
        time.sleep(1)

    if status != ProcessStatus.STARTED:
        raise RuntimeError("Tempory pod failed: {}".format(pod_name))
    """

    if parts_src[0]:
        new_src = "{}:{}".format(pod_name, parts_src[1])
        new_dst = dst
    elif parts_dst[0]:
        new_src = src
        new_dst = "{}:{}".format(pod_name, parts_dst[1])

    cmd = "kubectl cp {} {}".format(new_src, new_dst)
    os.system(cmd)

    # k8s_backend.terminate_job(job)


def detect_platforms():
    commands = ["gcloud", "aws"]
    platforms = ["gcp", "aws"]
    found_platforms = []

    for i, cmd in enumerate(commands):
        try:
            sp.check_call(["which", cmd], stdout=sp.DEVNULL)
        except sp.CalledProcessError:
            continue

        found_platforms.append(platforms[i])

    return found_platforms


def prompt_choices(choices, prompt):
    num = 0
    n = len(choices)

    if n > 1:
        for i, choice in enumerate(choices):
            print(i + 1, choice)

        while True:
            end = len(choices)
            input_str = input("{}? [1-{}] ".format(prompt, end))

            try:
                num = int(input_str) - 1
                if num < 0 or num >= end:
                    raise ValueError
                break

            except (TypeError, ValueError):
                print(
                    "Invalid input: {}. Please choose from [1-{}]".format(
                        input_str, end
                    )
                )
                continue

    return choices[num]


class DockerImageBuilder:
    def __init__(self, registry=""):
        self.registry = registry

    def get_docker_registry_image_name(image_base_name):
        return image_base_name

    def build(self):
        files = find_docker_files()
        n = len(files)
        if n == 0:
            raise RuntimeError("No docker files found in current directory")

        dockerfile = select_docker_file(files)

        cwd = os.path.basename(os.getcwd())

        # Use current dir as image base name
        image_base_name = cwd
        self.image_base_name = image_base_name

        sp.check_call(
            "docker build -f {} . -t {}".format(dockerfile, image_base_name),
            shell=True,
        )

        self.image_name = "{}:latest".format(image_base_name)

        self.tag()
        self.push()

        return self.full_image_name

    def tag(self):
        self.full_image_name = self.image_name

    def push(self):
        sp.check_call(
            "docker push {}".format(self.full_image_name), shell=True,
        )

    def docker_tag(self, in_name, out_name):
        sp.check_call("docker tag {} {}".format(in_name, out_name), shell=True)


class AWSImageBuilder(DockerImageBuilder):
    def __init__(self, registry):
        self.registry = registry
        parts = registry.split(".")
        self.region = parts[-3]

    def tag(self):
        image_name = self.image_name
        full_image_name = "{}/{}".format(self.registry, self.image_name)

        self.docker_tag(image_name, full_image_name)

        self.full_image_name = full_image_name
        return full_image_name

    def need_new_repo(self):
        output = sp.check_output(
            "aws ecr describe-repositories --region {}".format(self.region),
            shell=True,
        )
        res = json.loads(output)

        if "repositories" not in res:
            return True

        repos = res["repositories"]
        for repo in repos:
            # use self.image_base_name as repo name
            if repo["repositoryName"] == self.image_base_name:
                return False

        return True

    def create_repo_if_needed(self):
        if self.need_new_repo():
            sp.check_call(
                "aws ecr create-repository --region {} --repository-name {}".format(
                    self.region, self.image_base_name
                ),
                shell=True,
            )

        return

    def push(self):
        self.create_repo_if_needed()

        try:
            super().push()
        except sp.CalledProcessError:
            raise RuntimeError(
                "Failed to push images {}. "
                "Most likely your authorization token has expired. \nPlease run "
                '"aws ecr get-login-password --region {} | docker login --username AWS --password-stdin {}" to authenticate'.format(
                    self.full_image_name, self.region, self.registry
                )
            )


class GCPImageBuilder(DockerImageBuilder):
    def __init__(self, registry="gcr.io"):
        self.registry = registry

    def tag(self):
        image_name = self.image_name
        proj = get_default_project_gcp()

        full_image_name = "{}/{}/{}".format(self.registry, proj, image_name)
        self.docker_tag(image_name, full_image_name)

        self.full_image_name = full_image_name

        return full_image_name


@click.command(context_settings=dict(ignore_unknown_options=True,))
@click.option("-a", "--attach", is_flag=True)
@click.option("--image")
@click.option("--gpu")
@click.option("--cpu")
@click.option("--memory")
@click.option("-v", "--volume")
@click.argument("args", nargs=-1)
def run(attach, image, gpu, cpu, memory, volume, args):
    """Run a command on a kubernetes cluster with fiber."""
    platform = CONFIG["platform"]
    print(
        'Running "{}" on Kubernetes cluster ({}) '.format(
            " ".join(args), platform
        )
    )

    if image:
        full_image_name = image
    else:
        if platform == "gcp":
            builder = GCPImageBuilder()
        elif platform == "aws":
            registry = CONFIG["docker_registry"]
            if not registry:
                registry = input(
                    "What docker registry do you plan to use? "
                    "AWS registry format: "
                    '"[aws_account_id].dkr.ecr.[region].amazonaws.com"\n> '
                )
            builder = AWSImageBuilder(registry)
        else:
            raise ValueError('Unknow platform "{}"'.format(platform))

        full_image_name = builder.build()

    # run this to refresh access tokens
    exitcode = os.system("kubectl get po > /dev/null")

    job_name = os.path.basename(os.getcwd())

    k8s_backend = get_backend(platform)

    job_spec = core.JobSpec(image=full_image_name, name=job_name, command=args,)
    if gpu:
        job_spec.gpu = gpu

    if cpu:
        job_spec.cpu = cpu

    if memory:
        job_spec.mem = memory

    if volume:
        volumes = {volume: {"mode": "rw", "bind": "/persistent"}}
        job_spec.volumes = volumes

    job = k8s_backend.create_job(job_spec)
    pod_name = job.data.metadata.name
    exitcode = 0

    print("Created pod: {}".format(pod_name))

    if attach:
        # wait until job is running
        """
        os.system(
            "kubectl wait --for=condition=Ready pod/{}".format(pod_name)
        )
        """

        exitcode = os.system("kubectl logs -f {}".format(pod_name))

    if exitcode != 0:
        return exitcode

    return 0


def auto_select_platform():
    platforms = detect_platforms()
    if len(platforms) > 1:
        choice = prompt_choices(
            platforms,
            "Found many providers, which provider do you want to use",
        )
    elif len(platforms) == 1:
        choice = platforms[0]
    else:
        choice = prompt_choices(
            ["gcp", "aws"], "Which provider do you want to use"
        )

    return choice


@click.group()
@click.option("-d", "--docker-registry")
@click.option("--aws", is_flag=True, help="Run commands on Amazon AWS")
@click.option(
    "--gcp", is_flag=True, help="Run commands on Google Cloud Platform"
)
@click.version_option(version=fiber.__version__, prog_name="fiber")
def main(docker_registry, aws, gcp):
    """fiber command line tool that helps to manage workflow of distributed
    fiber applications.
    """
    if docker_registry is not None:
        CONFIG["docker_registry"] = docker_registry
    else:
        CONFIG["docker_registry"] = None

    platforms = [aws, gcp]
    platform_names = ["aws", "gcp"]
    n = sum(platforms)
    if n > 1:
        raise ValueError(
            'Only one of "{}" can be set'.format(
                ", ".join(["--" + p for p in platform_names])
            )
        )

    if aws:
        CONFIG["platform"] = "aws"
    elif gcp:
        CONFIG["platform"] = "gcp"
    else:
        CONFIG["platform"] = auto_select_platform()


main.add_command(run)
main.add_command(cp)


if __name__ == "__main__":
    main()
