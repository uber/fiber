import os
import re
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
import sys
from subprocess import check_call

install_requires = [
    "nnpy-bundle",
    "psutil",
    "docker <= 4.2.2",
    "cloudpickle",
    "kubernetes == 10.0.1",
    "requests",
    "click",
]

tests_require = [
    "pytest",
    "docker <= 4.2.2",
    "flake8",
]

extras = {
    "test": tests_require,
}


def find_version(*file_paths):
    basedir = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(basedir, *file_paths)) as fp:
        content = fp.read()
        version_match = re.search(
            r"^__version__ = ['\"]([^'\"]*)['\"]", content, re.M
        )
        if version_match:
            return version_match.group(1)

        raise RuntimeError("Version string not found.")


def read_long_description(filename="README.md"):
    with open(filename) as f:
        return f.read().strip()


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest

        errcode = pytest.main(self.test_args)
        sys.exit(errcode)


setup(
    author="Jiale Zhi",
    author_email="jiale@uber.com",
    description="A distributed computing library for modern computer clusters",
    include_package_data=True,
    install_requires=install_requires,
    extras_require=extras,
    tests_require=tests_require,
    cmdclass={"test": PyTest},
    license="Apache 2.0",
    long_description=read_long_description(),
    long_description_content_type="text/markdown",
    name="fiber",
    packages=find_packages(),
    entry_points={
        "console_scripts": ["fiber=fiber.cli:main"],
    },
    version=find_version("fiber", "__init__.py"),
    zip_safe=False,
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Topic :: System :: Distributed Computing",
    ],
    url="https://github.com/uber/fiber",
)
