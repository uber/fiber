#!/usr/bin/env bash

ulimit -n 8192

pytest tests
