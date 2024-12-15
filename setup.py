
from setuptools import setup, find_packages
import os

setup(
    name="custom_operators",  # Replace with your project name
    version="1.0.0",
    author="ram posam",
    author_email="posamram@gmail.com",
    description="Airflow Custom Operators",
    long_description=open("README.md").read() if os.path.exists("README.md") else "",
    long_description_content_type="text/markdown",
    url="https://rposam-devops@dev.azure.com/rposam-devops/devops-project/_git/custom_operators",
    packages=find_packages(),  # Automatically find packages (directories with __init__.py)
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]
)
