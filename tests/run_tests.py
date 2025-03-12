#!/usr/bin/env python3
"""
Test runner for the Solana Trading Strategy Simulator.

This script discovers and runs all tests in the tests directory.
"""

import unittest
import os
import sys

# Add the project root to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def run_tests():
    """Discover and run all tests in the tests directory."""
    print("=" * 70)
    print("Running tests for Solana Trading Strategy Simulator")
    print("=" * 70)

    # Discover all tests
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover(start_dir=os.path.dirname(__file__), pattern="test_*.py")

    # Run the tests
    test_runner = unittest.TextTestRunner(verbosity=2)
    result = test_runner.run(test_suite)

    # Return appropriate exit code
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(run_tests())
