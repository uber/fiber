## Supported platforms

Currently the following platforms are supported:

* Operating system: Linux, MacOS (local backend only)
* Python: 3.6+
* Supported cluster management systems:
	* Kubernetes (Tested with Google Kubernetes Engine on Google cloud)

We are interested in supporting other cluster management systems like
[Slurm](https://slurm.schedmd.com/), if you want to contribute to it please let
us know.


## Different fiber backends

Fiber provides 3 built-in backends currently:

####local

This backend is the default backend used by Fiber. It starts new processes
locally on your computer. The child process shares the same running environment
as the parent process. When using this backend, Fiber works the same as
`multiprocessing`.

The `local` backend works on Linux and MacOS.

#### docker

This backend starts new processes with docker containers. This backend still
works locally but compared to `local` backend, `docker` backend runs child
processes in a completed isolated docker environment. You need to build a
docker image for your project before running processes with this backend.
Examples of using this backend can be found
[here](/getting-started/#containerize-your-program). This
backend is usually used as a way to test your containerized application before
you run everything on a large scale on a computer cluster.

The `docker` backend currently only works on Linux and doesn't work on MacOS,
we will add support for it soon.

####kubernetes

This backend is the backend to use when you want to run your application on
Kubernetes cluster. When starting new processes, it creates a new
[Kubernetes pod](https://kubernetes.io/docs/concepts/workloads/pods/pod/) for
it. Each process also runs in an isolated environment. You also need to build a
docker image and push it to the appropriate container image registry for your
Kubernetes platform. Example usage of this backend can be found
[here](/getting-started/#running-on-a-computer-cluster).

See how to configure Fiber to use each backend [here](config.md)

## Automatic backend selection

Fiber also supports automatically detect which backend to use when no backend
is explicitly set. When fiber runs on a non-cluster environment, it uses `local`
backend by default. When it runs inside a Kubernetes pod, it will select
`kubernetes` as the backend to use.

