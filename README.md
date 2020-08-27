<p align="right">
  <a href="https://travis-ci.com/uber/fiber">
      <img src="https://travis-ci.com/uber/fiber.svg?token=BxMzxQEDDtTBPG9151kk&branch=master" alt="build" />
  </a>
</p>

<img src="docs/img/fiber_logo.png" alt="drawing" width="550"/>

[**Project Home**](https://uber.github.io/fiber/) &nbsp;
[**Blog**](https://eng.uber.com/fiberdistributed/) &nbsp;
[**Documents**](https://uber.github.io/fiber/getting-started/) &nbsp;
[**Paper**](https://arxiv.org/abs/2003.11164) &nbsp;
[**Media Coverage**](https://venturebeat.com/2020/03/26/uber-details-fiber-a-framework-for-distributed-ai-model-training/)

<img src="https://github.com/uber/fiber/raw/docs/email-list/docs/img/new-icon.png"/> Join Fiber users email list [fiber-users@googlegroups.com](https://groups.google.com/forum/#!forum/fiber-users)

# Fiber

### Distributed Computing for AI Made Simple

*This project is experimental and the APIs are not considered stable.*

Fiber is a Python distributed computing library for modern computer clusters.

* It is easy to use. Fiber allows you to write programs that run on a computer cluster level without the need to dive into the details of computer cluster.
* It is easy to learn. Fiber provides the same API as Python's standard [multiprocessing](https://docs.python.org/3.6/library/multiprocessing.html) library that you are familiar with. If you know how to use multiprocessing, you can program a computer cluster with Fiber.
* It is fast. Fiber's communication backbone is built on top of [Nanomsg](https://nanomsg.org/) which is a high-performance asynchronous messaging library to allow fast and reliable communication.
* It doesn't need deployment. You run it as the same way as running a normal application on a computer cluster and Fiber handles the rest for you.
* It it reliable. Fiber has built-in error handling when you are running a pool of workers. Users can focus on writing the actual application code instead of dealing with crashed workers.

Originally, it was developed to power large scale parallel scientific computation projects like [POET](https://eng.uber.com/poet-open-ended-deep-learning/) and it has been used to power similar projects within Uber.


## Installation

```
pip install fiber
```

Check [here](https://uber.github.io/fiber/installation/) for details.

## Quick Start


### Hello Fiber
To use Fiber, simply import it in your code and it works very similar to multiprocessing.

```python
import fiber

if __name__ == '__main__':
    fiber.Process(target=print, args=('Hello, Fiber!',)).start()
```

Note that `if __name__ == '__main__':` is necessary because Fiber uses *spawn* method to start new processes. Check [here](https://stackoverflow.com/questions/50781216/in-python-multiprocessing-process-do-we-have-to-use-name-main) for details.

Let's take look at another more complex example:

### Estimating Pi


```python
import fiber
import random

@fiber.meta(cpu=1)
def inside(p):
    x, y = random.random(), random.random()
    return x * x + y * y < 1

def main():
    NUM_SAMPLES = int(1e6)
    pool = fiber.Pool(processes=4)
    count = sum(pool.map(inside, range(0, NUM_SAMPLES)))
    print("Pi is roughly {}".format(4.0 * count / NUM_SAMPLES))

if __name__ == '__main__':
    main()
```


Fiber implements most of multiprocessing's API including `Process`, `SimpleQueue`, `Pool`, `Pipe`, `Manager` and it has its own extension to the multiprocessing's API to make it easy to compose large scale distributed applications. For the detailed API guild, check out [here](https://uber.github.io/fiber/process/).

### Running on a Kubernetes cluster

Fiber also has native support for computer clusters. To run the above example on Kubernetes, fiber provided a convenient command line tool to manage the workflow.

Assume you have a working docker environment locally and have finished configuring [Google Cloud SDK](https://cloud.google.com/sdk/docs/quickstarts). Both `gcloud` and `kubectl` are available locally. Then you can start by writing a Dockerfile which describes the running environment.  An example Dockerfile looks like this:

```dockerfile
# example.docker
FROM python:3.6-buster
ADD examples/pi_estimation.py /root/pi_estimation.py
RUN pip install fiber
```
**Build an image and launch your job**

```
fiber run -a python3 /root/pi_estimation.py
```

This command will look for local Dockerfile and build a docker image and push it to your Google Container Registry . It then launches the main job which contains your code and runs the command `python3 /root/pi_estimation.py` inside your job. Once the main job is running, it will start 4 subsequent jobs on the cluster and each of them is a Pool worker.


## Supported platforms

* Operating system: Linux
* Python: 3.6+
* Supported cluster management systems:
	* Kubernetes (Tested with Google Kubernetes Engine on Google cloud)

We are interested in supporting other cluster management systems like [Slurm](https://slurm.schedmd.com/), if you want to contribute to it please let us know.


Check [here](https://uber.github.io/fiber/platforms/) for details.

## Documentation

The documentation, including method/API references, can be found [here](https://uber.github.io/fiber/getting-started/).


## Testing

Install test dependencies. You'll also need to make sure [docker](https://docs.docker.com/install/) is available on the testing machine.

```bash
$ pip install -e .[test]
```

Run tests

```bash
$ make test
```

## Contributing
Please read our [code of conduct](CODE_OF_CONDUCT.md) before you contribute! You can find details for submitting pull requests in the [CONTRIBUTING.md](CONTRIBUTING.md) file. Issue [template](https://help.github.com/articles/about-issue-and-pull-request-templates/).

## Versioning
We document versions and changes in our changelog - see the [CHANGELOG.md](CHANGELOG.md) file for details.

## License
This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

## Cite Fiber

```
@misc{zhi2020fiber,
    title={Fiber: A Platform for Efficient Development and Distributed Training for Reinforcement Learning and Population-Based Methods},
    author={Jiale Zhi and Rui Wang and Jeff Clune and Kenneth O. Stanley},
    year={2020},
    eprint={2003.11164},
    archivePrefix={arXiv},
    primaryClass={cs.LG}
}
```

## Acknowledgments
* Special thanks to Piero Molino for designing the logo for Fiber
