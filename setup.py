#!/usr/bin/env python3

import os
from setuptools import setup

from btrfs_backup import __version__

# read the contents of README file
from pathlib import Path
project_directory = Path(__file__).parent
long_description = (project_directory / "README.md").read_text()

setup(
    name="btrfs-backup-ng",
    version=__version__,
    description="Intelligent, feature-rich backups for btrfs",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/berrym/btrfs-backup-ng",
    author="Michael Berry",
    author_email="trismegustis@gmail.com",
    license="MIT",
    packages=["btrfs_backup", "btrfs_backup.endpoint"],
    zip_safe=False,
    entry_points={
       "console_scripts": [
            "btrfs-backup-ng = btrfs_backup.__main__:main",
        ],
    },
)
