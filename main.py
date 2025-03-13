#!/usr/bin/env python3
"""
Solana Trading Simulator

Main entry point for the Solana Trading Simulator.
This script provides the primary interface to all functionality.

Usage:
    python main.py [command] [options]

Examples:
    python main.py simulate --max-pools 20
    python main.py cache import --limit 10
    python main.py cache status
"""

import sys
from src.cli.main import main as cli_main

if __name__ == "__main__":
    # Delegate to the CLI main function
    sys.exit(cli_main())
