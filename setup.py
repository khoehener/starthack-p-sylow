# setup.py
from setuptools import setup, find_packages

setup(
    name="file_parser",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "requests",
        "pandas",
        "anthropic",  # for claude
        "openpyxl",   # for excel files
    ],
)