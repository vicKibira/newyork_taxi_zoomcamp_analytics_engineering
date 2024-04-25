# Makefile for ny_taxi_data

.PHONY: test run clean

# Compiler and flags
PYTHON = python3

# Source files
PY_FILES = load_data.py
TEST_FILES = test_ny_taxi_data.py

# Targets
test: $(PY_FILES) $(TEST_FILES)
	$(PYTHON) -m pytest

run: $(PY_FILES)
	$(PYTHON) $(PY_FILES)

clean:
	rm -rf __pycache__ .pytest_cache
