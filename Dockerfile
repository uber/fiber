FROM python:3.6.8-stretch
RUN pip3 install fiber
ADD . /work/
RUN cd /work && pip3 install -e .[test]
RUN cd /work && pip3 install -e .
