from __future__ import absolute_import, division, print_function

import io
import os
import re

from setuptools import find_packages, setup

# cf.
# https://packaging.python.org/guides/single-sourcing-package-version/#single-sourcing-the-version
def read(*names, **kwargs):
  with io.open(
    os.path.join(os.path.dirname(__file__), *names),
    encoding=kwargs.get("encoding", "utf8")
  ) as fp:
    return fp.read()

def find_version(*file_paths):
  version_file = read(*file_paths)
  version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                            version_file, re.M)
  if version_match:
    return version_match.group(1)
  raise RuntimeError("Unable to find version string.")

setup(
    name='datasync',
    version=find_version("datasync", "__init__.py"),
    description='Python package for synching metadata into an ISPyB database',
    long_description='This package provides a framework for synching certain kinds of metadata from other data sources into an ISPyB database.',
    url='https://github.com/DiamondLightSource/ispyb-propagation',
    author='Karl Erik Levik',
    author_email='scientificsoftware@diamond.ac.uk',
    download_url='https://github.com/DiamondLightSource/ispyb-propagation/releases',
    keywords = ['ISPyB', 'database'],
    packages=find_packages(),
    license='Apache License, Version 2.0',
    install_requires=[
      'cx_Oracle',
      'mysql-connector<2.2.3',
    ],
    setup_requires=[
      'pytest-runner',
    ],
    tests_require=[
      'mock',
      'pytest',
    ],
    classifiers = [
      'Development Status :: 3 - Alpha',
      'License :: OSI Approved :: Apache Software License',
      'Programming Language :: Python :: 2',
      'Programming Language :: Python :: 2.7',
      'Programming Language :: Python :: 3',
      'Programming Language :: Python :: 3.3',
      'Programming Language :: Python :: 3.4',
      'Programming Language :: Python :: 3.5',
      'Programming Language :: Python :: 3.6',
      'Operating System :: OS Independent',
      'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
