#
# Copyright (c) 2026 Tripti Sharma
# Licensed under the MIT License
#
from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk>=0.80.0",
    "requests>=2.31.0",
]

TEST_REQUIREMENTS = [
    "pytest~=6.1",
    "pytest-mock~=3.6",
    "requests-mock~=1.9",
]

setup(
    name="source_clevertap",
    version="0.1.0",
    description="Source implementation for CleverTap Profiles API",
    author="Tripti Sharma",
    author_email="tripti0777@gmail.com",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"source_clevertap": ["schemas/*.json", "spec.yaml"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)

