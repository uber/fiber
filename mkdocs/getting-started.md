In this guide, we will walk through the basic features of Fiber. By the end of this guide, you should be able to launch your own Fiber application on a Kubernetes cluster!

## A minimal example

If you have already [installed](installation.md) Fiber on your computer and your environment is [supported by Fiber](platforms.md), then we can get things started.

Open your favorite editor and create a Python file called `hello_fiber.py` with the following content:

```python
import fiber

if __name__ == '__main__':
    fiber.Process(target=print, args=('Hello, Fiber!',)).start()
```

You may find that the API is the same as Python's [multiprocessing](https://docs.python.org/3.6/library/multiprocessing.html) library. In fact, most of multiprocessing's API is supported by Fiber. You can take an program that is written with multiprocessing and changes a few lines to make it work with Fiber. We will see [some examples](examples.md) later.

Run it with the following command:

```bash
python hello_fiber.py
```

You should see the following output:

```
Hello, Fiber!
```

Under the hood, what Fiber does is that it launches a new process locally on your computer, and then run `print` function with arguments `'Hello, Fiber!'`.

"Isn't this just multiprocessing?", you may ask. Indeed, Fiber works the same as multiprocessing when running locally on a single computer. But we will show how this simple design can be powerful when you run things on a computer cluster.


## A more complex example

The previous example is too easy, isn't it? Let's do something more complex. In this example, we will 
create a simple program that estimates Pi with Monte Carlo Method.

Create a new file called `pi_estimation.py` with the following content:

```python
from fiber import Pool
import random

NUM_SAMPLES = int(1e6)

def is_inside(p):
    x, y = random.random(), random.random()
    return x * x + y * y < 1

def main():
    pool = Pool(processes=4)
    pi = 4.0 * sum(pool.map(is_inside, range(0, NUM_SAMPLES))) / NUM_SAMPLES
    print("Pi is roughly {}".format(pi))

if __name__ == '__main__':
    main()
```

In this example, we use [Monte Carlo method](https://en.wikipedia.org/wiki/Monte_Carlo_method) to [estimate](https://academo.org/demos/estimating-pi-monte-carlo/) the value of Pi.

Run it with the following command:

```bash
python pi_estimation.py
```

You should see something like this:

```
Pi is roughly 3.140636
```

What Fiber does is it created a pool of 4 workers, pass all the workload to them and collect results from them. In this example, each worker calculates whether a single random point is inside a circle or not. And we can increase the degree of degree of parallelism by increasing the number of Pool workers.

## Containerize your program

Before we can run our program on a computer cluster, we need to first encapsulate our computation environment in a [container](https://www.docker.com/resources/what-container). It is a method of virtualization that package an application's code and dependencies into a single object. The aim is to allow application to run reliably and consistently from one environment to another environment.

For Fiber, the benefits of using a container includes:

* container is portable and we can run it locally and on a remote cluster
* container encapsulates the running environment of the program, and this makes sure that if something works locally, it most likely will also work on a computer cluster.

This step requires [docker](https://www.docker.com/), check out [here](https://docs.docker.com/install/) to see how it can be installed on your system. To encapsulate our Pi estimation program, we create a file called `Dockerfile` for it.


```dockerfile
# Dockerfile
FROM python:3.6-buster
ADD pi_estimation.py /root/pi_estimation.py
RUN pip install fiber
```

Make sure your current directory doesn't include anything irreverent or you can ignore them from the build by putting them in a [`.dockerignore`](https://docs.docker.com/engine/reference/builder/#dockerignore-file) file.

Build an image by running the following command:

```bash
docker build -t fiber-pi-estimation .
```

After the image is built, you will get an docker image called `fiber-pi-estimation:latest`.

## Test your container

With this image built, you can test if your program works inside the docker by running it with Fiber's docker backend. Fiber provides many different backends for different running environment. Checkout [here](platforms.md) for details.

You can run the following command to test if your recently build container works or not by running the following command:

```bash
FIBER_BACKEND=docker FIBER_IMAGE=fiber-pi-estimation:latest python pi_estimation.py
```

You should see the familiar output and it looks like:

```
Pi is roughly 3.142324
```

## Fiber config

So what's the difference between this one and the previous run? In this run, we use special environment variables to tell Fiber what backend to use and what docker image to use. These environment variables are a part of Fiber's configuration system.

`FIBER_BACKEND` tells Fiber what backend to use. Currently, Fiber supports these backends: `local`, `docker` and `kubernetes`. When `FIBER_BACKEND` is set to `docker`, all new processes will be launched through `docker` backend which means all of them will be running inside their own docker container.

`FIBER_IMAGE` tells Fiber what docker image to use when launching new containers. This container provides the running environment for your child processes, so it needs to have Fiber installed in it. And we already did that in the previous step when building the docker container.

Note that in this example, the master process (the one you started with `python pi_estimation.py`) still runs on local machine instead of inside a docker container. All the processes started by Fiber are inside containers.

You can checkout the containers launched by Fiber by running:

```
docker ps -a|grep fiber-pi-estimation
```
Alternatively, you can also create a `.fiberconfig` file to pass the [configurations](config.md) to Fiber. The equivalent config file is:

```
#.fiberconfig
backend=docker
image=fiber-pi-estimation
```

To learn more about Fiber's configuration system, check out [here](config.md).

## Running on a computer cluster

With all the previous steps finished, now it's time to try some distributed computing on a real computer cluster. The good thing is that most of the work has already been done by now.

Here we use [Google Kubernetes Engine](https://cloud.google.com/kubernetes-engine) on [Google Cloud](https://cloud.google.com/) as an example. You'll need to install [Google Cloud SDK](https://cloud.google.com/sdk/docs) and [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/#download-as-part-of-the-google-cloud-sdk) on your machine. Also, you need to authenticate docker to access Google Container Registry (GCR) following this [guide](https://cloud.google.com/container-registry/docs/pushing-and-pulling).

We first config the cluster to grant permission to the default service account so that Fiber can access Kubernetes API from within the cluster.

```bash
kubectl apply -f https://raw.githubusercontent.com/uber/fiber/master/configs/rbac.yaml
```

Then we tag our image and push it to a container registry that is accessible by your Kubernetes cluster.
```bash
docker tag fiber-pi-estimation:latest gcr.io/[your-project-name]/fiber-pi-estimation:latest
docker push gcr.io/[your-project-name]/fiber-pi-estimation:latest
```

Now that the docker image is available, we can launch our job by:

```bash
kubectl create job fiber-pi-estimation --image=gcr.io/[your-project-name]/fiber-pi-estimation:latest -- python3 /root/pi_estimation.py
```

We should see something like this in the output:

```
job.batch/fiber-pi-estimation created
```

The job has been submitted to Kubernetes cluster, and now we can get its logs. It may take some time before the job is scheduled.

```bash
kubectl logs $(kubectl get po|grep fiber-pi-estimation|awk '{print $1}')
```

And you should see this familiar output from the above command:

```
Pi is roughly 3.139972
```

Congratulations! You have successfully run your first Fiber program on Kubernetes.

On Kubernetes, Fiber behaves similarly to when running locally with Docker. Each process becomes a Kubernetes pod and all the pods work collectively to compute our estimation of Pi!


## Running with `fiber` command

If the above process looks too complex for you, we have a better solution. To simplify the workflow of running jobs on Kubernetes, we create a command-line tool named `fiber` to help to manage the job running process. Specifically, `fiber run` command can help you build docker images, push images to your GCR and run jobs for you. It currently only works for Google Cloud but we plan to extend it to work with other platforms.

With `fiber` command, running a fiber program on Kubernetes is very simple. `fiber run` looks for a valid docker file on the current directory, builds a docker image with that, tag it and push it to GCR with your default google cloud project name, and then run the command that you passed to `fiber run`.

We will re-use our previously written `Dockerfile`. Instead of building, pushing images and launch jobs by our own, now everything can be simplified to one command:

```bash
fiber run python3 /root/pi_estimation.py
```

Note that we use `/root/pi_estimation.py` is the path of the source file inside docker image. We should see something like this in the last line of the output:

```
Created pod: fiber-16fb282d
```

And `fiber-16fb282d` is the Kubernetes pod that we created for our main process. To get the output of our job, simply run:

```bash
kubectl logs fiber-16fb282d
```

And we should see the familiar output:

```
Pi is roughly 3.141044
```

For detailed docs on `fiber` command, check out [here](cli.md).

## Resource Limits

Now that we can run our program on a computer cluster, it's important to think about how much resources each of the processes is going to use so that all the jobs can be properly scheduled. We can specify how many CPU cores, how much memory and how many GPU one process needs by using `fiber.meta` API.

Take our Pi estimation program as an example. Because our `is_inside` can only use one CPU and doesn't need much memory, we can specify these resource limits by using `@fiber.meta` decorator. A modified version of `pi_estimation.py` looks like this:

```python
import fiber
from fiber import Pool
import random

NUM_SAMPLES = int(1e6)

# 1 CPU core and 1000M of memory
@fiber.meta(cpu=1, memory=1000)
def is_inside(p):
    x, y = random.random(), random.random()
    return x * x + y * y < 1

def main():
    pool = Pool(processes=4)
    pi = 4.0 * sum(pool.map(is_inside, range(0, NUM_SAMPLES))) / NUM_SAMPLES
    print("Pi is roughly {}".format(pi))

if __name__ == '__main__':
    main()
```

We now run this new version with `fiber run` command:

```bash
$ fiber run python3  /root/pi_estimation.py
...
Created pod: fiber-36c2da03
```

And then we get the logs:

```
$ kubectl logs fiber-36c2da03
Pi is roughly 3.141004
```

