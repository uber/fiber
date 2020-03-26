## Run OpenAI Baselines on Kubernetes with Fiber

In this example, we'll show you how to integrate fiber with OpenAI baselines with just **one line** of code change.

If your project is already using Python's multiprocessing, then integrate it with Fiber is very easy. Here, we are going to use [OpenAI Baselines](https://github.com/openai/baselines) as an example to show how to easily run code written with multiprocessing on Kubernetes easily.

### Prepare the code

First, we clone baselines from Github, create a new branch and setup our local environment:

```bash
git clone https://github.com/openai/baselines
cd baselines
git checkout -b fiber
virtualenv -p python3 env
. env/bin/activate
echo "env" > .dockerignore
pip install "tensorflow<2"
pip install -e .
pip install fiber
```

Test that if the environment works:

```baseh
python -m baselines.run --alg=ppo2 --env=CartPole-v0 --network=mlp --num_timesteps=10000
```

If it works, you should see something like this in your output
```
---------------------------------------
| eplenmean               | 23.8      |
| eprewmean               | 23.8      |
| fps                     | 1.95e+03  |
| loss/approxkl           | 0.000232  |
| loss/clipfrac           | 0         |
| loss/policy_entropy     | 0.693     |
| loss/policy_loss        | -0.00224  |
| loss/value_loss         | 48.4      |
| misc/explained_variance | -0.000784 |
| misc/nupdates           | 1         |
| misc/serial_timesteps   | 2.05e+03  |
| misc/time_elapsed       | 1.05      |
| misc/total_timesteps    | 2.05e+03  |
---------------------------------------
```

OpenAI baselines has a `SubprocVecEnv` that, according to it's documentation, runs multiple environments in parallel in subprocesses and communicates with them via pipes. We'll start from here to modify it to work with Fiber:

### Fiberization (Or adapt your code to run with Fiber)

Open `baselines/common/vec_env/subproc_vec_env.py` and change this line:

```Python
import multiprocessing as mp
```

to

```Python
import fiber as mp
```

Let's do a quick test to see if this change works

```bash
python -m baselines.run --alg=ppo2 --env=CartPole-v0 --network=mlp --num_timesteps=10000 --num_env 2
```

We set `--num_env 2` to make sure baselines is using `SubprocVecEnv`. If everything works, we should see similar output as the previous run.

### Containerize the application

OpenAI baselines already has a Dockerfile available, so we just need to add fiber to it by adding a line `RUN pip install fiber`. After modification, the Dockerfile looks like this:

```Dockerfile
FROM python:3.6

RUN apt-get -y update && apt-get -y install ffmpeg
# RUN apt-get -y update && apt-get -y install git wget python-dev python3-dev libopenmpi-dev python-pip zlib1g-dev cmake python-opencv

ENV CODE_DIR /root/code

COPY . $CODE_DIR/baselines
WORKDIR $CODE_DIR/baselines

# Clean up pycache and pyc files
RUN rm -rf __pycache__ && \
    find . -name "*.pyc" -delete && \
    pip install 'tensorflow < 2' && \
    pip install -e .[test]

RUN pip install fiber

CMD /bin/bash
```

It's a good habit to make sure everything works locally before submitting the job to the bigger cluster because this will save you a lot of debugging time. So we build our docker image locally:

```
docker build -t fiber-openai-baselines .
```

When Fiber starts new dockers locally, it will mount your home directory into docker. So we need to modify baselines' log dir to make sure it can write logs to the correct place by adding an argument `--log_path=logs`. By default, baselines writes to `/tmp` dir which is not shared by Fiber master process and subprocesses. We also add `--num_env 2` to make sure baselines uses `SubprocVecEnv` so that Fiber processes can be launched.


```bash
FIBER_BACKEND=docker FIBER_IMAGE=fiber-openai-baselines:latest python -m baselines.run --alg=ppo2 --env=CartPole-v0 --network=mlp --num_timesteps=10000 --num_env 2 --log_path=logs
```

### Running on Kubernetes

Now let's run our fiberized OpenAI baselines on Kubernetes. This time we run `1e7` time steps. Also, we want to store the output of the run on persistent storage. We can do this with `fiber` command's [mounting persistent volumes](advanced.md#working-with-persistent-storage) feature.


```bash
$ fiber run -v fiber-pv-claim python -m baselines.run --alg=ppo2 --env=CartPole-v0 --network=mlp --num_timesteps=1e7 --num_env 2 --log_path=/persistent/baselines/logs/
```

It should output something like this:

```
Created pod: baselines-d00eb2ef
```

After the job is done, you can copy the logs with these commands:

```bash
$ fiber cp fiber-pv-claim:/persistent/baselines/logs baselines-logs
```
