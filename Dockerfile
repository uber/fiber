#from python:3.6.8-stretch
from ubuntu:18.04
RUN apt-get update && apt-get install -y python3-pip
RUN apt-get update && apt-get install -y gdb python2.7-dbg netcat vim net-tools git
ADD . /work/
RUN cd /work && pip3 install -e .[test]
RUN cd /work && pip3 install -e .
