"""btrfs-backup-ng: Automated btrfs backup management.

This is the main entry point that dispatches to either the new
subcommand-based CLI or the legacy positional argument CLI.

Copyright (c) 2024 Michael Berry <trismegustis@gmail.com>
Copyright (c) 2017 Robert Schindler <r.schindler@efficiosoft.com>
Copyright (c) 2014 Chris Lawrence <lawrencc@debian.org>

MIT License - See LICENSE file for details.
"""

import sys

from . import lifecycle
from .cli import main as cli_main


def main() -> None:
    """Main entry point for btrfs-backup-ng."""
    # Once, here, on the main thread: SIGTERM and SIGHUP stop this run's
    # children, release its locks and close its connections before the process
    # dies of the signal. Library code only registers what must be let go;
    # installing is the program's decision. SIGINT keeps Python's own handling.
    lifecycle.install_signal_handlers()
    try:
        sys.exit(cli_main())
    except KeyboardInterrupt:
        # Graceful exit on Ctrl+C without printing traceback
        print("\nInterrupted.")
        sys.exit(130)  # Standard exit code for SIGINT


if __name__ == "__main__":
    main()
