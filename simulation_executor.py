#!/usr/bin/env python3
"""Project-root entry point for the persistent simulated execution endpoint."""

import sys

from calcoord.cli import main


if __name__ == "__main__":
    raise SystemExit(main(["executor", *sys.argv[1:]]))
