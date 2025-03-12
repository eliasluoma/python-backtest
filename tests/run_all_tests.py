#!/usr/bin/env python3
"""
Test runner script for the Solana Trading Strategy Simulator.

This script discovers and runs all test cases in the project.
"""

import unittest
import sys
import logging
from pathlib import Path

# Configure logging for tests
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Add the project root directory to Python path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def discover_tests(start_dir="tests"):
    """
    Discover all test cases in the project.

    Args:
        start_dir: Directory to start discovery from

    Returns:
        Test suite containing all discovered tests
    """
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=start_dir, pattern="test_*.py")
    return suite


def run_all_tests():
    """
    Run all discovered tests and return exit code.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    # Print test run header
    print("\n" + "=" * 80)
    print("SOLANA TRADING STRATEGY SIMULATOR - TEST SUITE")
    print("=" * 80 + "\n")

    # Discover and run tests
    suite = discover_tests()
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Print test run summary
    print("\n" + "=" * 80)
    print(f"TEST SUMMARY: Ran {result.testsRun} tests")
    print(f"  Failures: {len(result.failures)}")
    print(f"  Errors: {len(result.errors)}")
    print(f"  Skipped: {len(result.skipped)}")
    print("=" * 80 + "\n")

    # Return exit code
    if result.wasSuccessful():
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(run_all_tests())
