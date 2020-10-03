This feature is WIP. To test this feature, create a config like this:

```
[default]
backend=ssh
ssh_nodes=user@ip
log_level=debug
log_file=stdout
spmd=False
```

Then run:

`ipython examples/pi_estimation.py`

The reason to use ipython is ipython can trigger Fiber to use cloudpickle.
