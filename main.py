##
# Copyright (c) 2026 Tripti Sharma
# Licensed under the MIT License
#!/usr/bin/env python3

import sys
import os

# Add the source_clevertap directory to the path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from airbyte_cdk.entrypoint import launch
from source_clevertap import SourceClevertap


def run():
    source = SourceClevertap()
    launch(source, sys.argv[1:])


if __name__ == "__main__":
    run()

