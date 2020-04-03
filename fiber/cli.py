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
import time
import sys
import subprocess
from pathlib import Path

import click

import fiber.core as core
from fiber.kubernetes_backend import Backend
from fiber.core import ProcessStatus


CONFIG = {}


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
    name = subprocess.check_output(
        "gcloud config list --format 'value(core.project)' 2>/dev/null",
        shell=True,
    )
    return name.decode("utf-8").strip()


def get_docker_registry_image_name(image, info):
    """Generate a full docker image name with registry information and tags."""
    if "aws" in info:
        # AWS registry
        region = info["aws"]["region"]
        registry = info["aws"]["registry"]
        os.system(
            "aws ecr create-repository --region {} --repository-name {}".format(
                region, image
            )
        )
        image_name = "{}/{}:latest".format(registry, image)
        return image_name

    elif "gcp" in info:
        # GCP registry
        proj = info["gcp"]["project"]
        registry = info["gcp"]["registry"]
        return "{}/{}/{}:latest".format(registry, proj, image)

    raise RuntimeError(
        "Bad info dict: {}. None of the supported keys ({}) are found.",
        info,
        ["aws", "gcp"],
    )


def build_docker_image(dockerfile, image_base_name, full_image_name, info):

    exitcode = os.system(
        "docker build -f {} . -t {}".format(dockerfile, image_base_name)
    )
    if exitcode != 0:
        sys.exit(exitcode)

    image_name = "{}:latest".format(image_base_name)
    exitcode = os.system("docker tag {} {}".format(image_name, full_image_name))
    if exitcode != 0:
        return exitcode

    exitcode = os.system("docker push {}".format(full_image_name))
    if exitcode != 0:
        if "aws" in info:
            region = info["aws"]["region"]
            registry = info["aws"]["registry"]
            raise RuntimeError(
                "Failed to push images {}."
                "If your authorization token has expired. \nPlease run "
                '"aws ecr get-login-password --region {} | docker login --username AWS --password-stdin {}" to authenticate'.format(
                    full_image_name, region, registry
                )
            )
        else:
            raise RuntimeError(
                "Failed to push images {}.".format(full_image_name)
            )

    return 0


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

    k8s_backend = Backend(incluster=False)
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
            subprocess.check_call(["which", cmd], stdout=subprocess.DEVNULL)
        except subprocess.CalledProcessError as e:
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


@click.command(context_settings=dict(ignore_unknown_options=True,))
@click.option("-a", "--attach", is_flag=True)
@click.option("--build/--no-build", default=True)
@click.option("--gpu")
@click.option("--cpu")
@click.option("--memory")
@click.option("-v", "--volume")
@click.argument("args", nargs=-1)
def run(attach, build, gpu, cpu, memory, volume, args):
    """Run a command on a kubernetes cluster with fiber."""
    print('Running "{}" on Kubernetes cluster'.format(" ".join(args)))

    files = find_docker_files()

    n = len(files)
    if n == 0:
        print("No docker files found")
        return 1

    cwd = os.path.basename(os.getcwd())
    image_base_name = cwd

    registry = CONFIG["docker_registry"]
    info = {}

    if registry is None:
        platforms = detect_platforms()
        if len(platforms) > 1:
            choice = prompt_choices(
                platforms,
                "Found many providers, which provider do you want to use",
                "providers",
            )
        elif len(platforms) == 1:
            choice = platforms[0]
        else:
            choice = prompt_choices(
                ["gcp", "aws"], "Which provider do you want to use", "providers"
            )

        if choice == "gcp":
            registry = "gcr.io"
        elif choice == "aws":
            registry = input(
                "What docker registry do you plan to use?\nFor AWS: [aws_account_id].dkr.ecr.[region].amazonaws.com\nFor GCP: gcr.io\n> "
            )
        else:
            # Should never go here
            raise ValueError("bad choice")

    if registry.endswith(".amazonaws.com"):
        parts = registry.split(".")
        region = parts[-3]

        info["aws"] = {
            "region": region,
            "registry": registry,
        }

    elif registry.endswith("gcr.io"):
        project = get_default_project_gcp()
        info["gcp"] = {
            "project": project,
            "registry": registry,
        }
    else:
        raise ValueError("Unrecognized docker registry: {}".format(registry))

    full_image_name = get_docker_registry_image_name(image_base_name, info)

    if build:
        dockerfile = select_docker_file(files)
        build_docker_image(dockerfile, image_base_name, full_image_name, info)

    # run this to refresh access tokens
    exitcode = os.system("kubectl get po > /dev/null")

    k8s_backend = Backend(incluster=False)
    job_spec = core.JobSpec(
        image=full_image_name, name=image_base_name, command=args,
    )
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


@click.group()
@click.option("-d", "--docker-registry")
def main(docker_registry):
    """fiber command line tool that helps to manage workflow of distributed
    fiber applications.
    """
    if docker_registry is not None:
        CONFIG["docker_registry"] = docker_registry
    else:
        CONFIG["docker_registry"] = None


main.add_command(run)
main.add_command(cp)


if __name__ == "__main__":
    main()
