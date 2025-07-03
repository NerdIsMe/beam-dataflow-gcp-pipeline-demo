#!/usr/bin/env python
from setuptools import find_packages
from setuptools import setup

setup(
    name='my_beam_pipeline',
    version='0.1',
    packages=find_packages(),
    include_package_data=True,
    description='My Apache Beam pipeline for GCP Dataflow',
)