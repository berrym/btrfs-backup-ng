# pyright: standard

"""btrfs-backup-ng: Automated btrfs backup management.

This is the main entry point that dispatches to either the new
subcommand-based CLI or the legacy positional argument CLI.

Copyright (c) 2024 Michael Berry <trismegustis@gmail.com>
Copyright (c) 2017 Robert Schindler <r.schindler@efficiosoft.com>
Copyright (c) 2014 Chris Lawrence <lawrencc@debian.org>

MIT License - See LICENSE file for details.
"""

import sys

from .cli import main as cli_main


def main() -> None:
    """Main entry point for btrfs-backup-ng."""
    sys.exit(cli_main())


if __name__ == "__main__":
    main()
