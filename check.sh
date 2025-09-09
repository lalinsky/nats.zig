#!/bin/bash

set -e

# Default values
TEST_FILTER=""
TEST_LOG_CAPTURE="true"
TEST_FAIL_FIRST="false"
TEST_VERBOSE="true"
CI_MODE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --test-filter)
            TEST_FILTER="$2"
            shift 2
            ;;
        --test-log-capture)
            TEST_LOG_CAPTURE="$2"
            shift 2
            ;;
        --test-fail-first)
            TEST_FAIL_FIRST="$2"
            shift 2
            ;;
        --test-verbose)
            TEST_VERBOSE="$2"
            shift 2
            ;;
        --ci)
            CI_MODE=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--test-filter \"test name\"] [--test-log-capture true/false] [--test-fail-first true/false] [--test-verbose true/false] [--ci]"
            exit 1
            ;;
    esac
done

echo "=== Formatting code ==="
if [ "$CI_MODE" = true ]; then
    echo "Checking formatting (CI mode)..."
    zig fmt --check .
else
    echo "Formatting code..."
    zig fmt .
fi

# Set up environment variables for tests
if [ -n "$TEST_FILTER" ]; then
    export TEST_FILTER
fi
export TEST_LOG_CAPTURE="$TEST_LOG_CAPTURE"
export TEST_FAIL_FIRST="$TEST_FAIL_FIRST" 
export TEST_VERBOSE="$TEST_VERBOSE"

echo "=== Running unit tests ==="
if [ -n "$TEST_FILTER" ]; then
    echo "Running unit tests with filter: $TEST_FILTER"
else
    echo "Running all unit tests..."
fi
zig build test-unit

echo "=== Running integration tests ==="
if [ -n "$TEST_FILTER" ]; then
    echo "Running integration tests with filter: $TEST_FILTER"
else
    echo "Running all integration tests..."
fi
zig build test-e2e

echo "=== All checks passed! ==="
