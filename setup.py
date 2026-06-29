
from setuptools import setup, find_packages
import os

setup(
    name="custom_operators",
    version="1.0.0",
    author="ram posam",
    author_email="posamram@gmail.com",
    description="Airflow Custom Operators",
    long_description=open("README.md").read() if os.path.exists("README.md") else "",
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/custom_operators",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]
)
