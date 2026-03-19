# setup.py
from setuptools import setup, find_packages

setup(
    name="file_parser",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
)