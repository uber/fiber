## Current problems

It is well known that doing distributed computing is hard. After talking to many people who run large scale distributed computing jobs on a daily basis, we found that there are a couple of reasons  why it is so hard to do distributed computing nowadays:

* There is a huge gap between making code work locally on laptops or desktops and running code on a production cluster. You can make MPI work locally but it's a completely different story for running it on a computer cluster.
* No dynamic scaling is available. If you launch a job that requires a large amount of resources, then most likely you'll need to wait until everything is allocated before you can run your job. This makes it less efficient.
* Error handling is missing. While running, some jobs may fail. And you may be put into a very nasty situation where you have to recover part of the result or discard the whole run.
* High learning cost. Each system has different APIs and ways of programming. In order to launch jobs with a new system, a user has to learn a set of completely different knowledge before jobs can be launched.
* Limitation in the programming model. Sometimes it's hard to make framework A work on framework B because the programming model of framework B is not compatible with framework A.

## How can Fiber help

After we understood why it's hard, the path for designing a better framework for distributed computing is clear. We need to make it easy to use and it needs to have very low friction between local development and remote run. And Fiber help to solve these problems by:

* Providing the same API when running locally on a desktop or on a computer cluster, reducing the frictions between local development and running on computer clusters
* Providing built-in ways of dynamically creating new jobs (processes), allowing computation to dynamically scale while running
* Handling job-level errors (job failed/rescheduling)  while processes are running
* Reusing the same APIs as Python’s multiprocessing library. If users are familiar with how to do parallel computing with Python’s multiprocessing, they can use Fiber. This greatly reduced learning cost. And also, because multiprocessing is Python's standard library, by reusing its API, it increased the probability that Fiber is compatible with other frameworks.
* Fiber doesn't need to be deployed. It's designed to be a library instead of a service. User's don't need to re-deploy their running environment if they added some new dependency to their code.

In addition to providing an easy to use user interface, Fiber is also built with performance in mind. We build Fiber's communication backbone with [Nanomsg](https://nanomsg.org/), a high-performance asynchronous messaging library. In addition, we also make sure that Fiber can be used together with other specialized frameworks in areas where performance is critical. Examples of this include distributed SGD where many existing frameworks like [Horovod](https://github.com/horovod/horovod), [torch.distributed](https://pytorch.org/docs/stable/distributed.html) have already provided very good solutions. Fiber can be used together with them using Fiber's [Ring feature](experimental/ring.md) to help to set up a distributed training job on computer clusters.
