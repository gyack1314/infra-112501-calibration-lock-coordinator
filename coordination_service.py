#!/usr/bin/env python3
"""Project-root entry point for the persistent coordination service."""

import sys

from calcoord.cli import main


if __name__ == "__main__":
    raise SystemExit(main(["service", *sys.argv[1:]]))
