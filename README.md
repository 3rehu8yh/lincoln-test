# Test project

```bash
# The following expects the Current Working Directory (CWD) to be
# the root of the project.

# Create a virtual environment for Python.
python3 -m venv .pyenv

# Check types.
mypy test_python test_sql

# Check formatting.
python3 -m yapf --diff test_python/*.py test_sql/*.py

# Format code.
python3 -m yapf -i test_python/*.py test_sql/*.py

# Run the tests.
pytest

# Runs the pipeline in test mode and puts its result in a file.
python3 -m test_python.pipeline > test_python/expected_graph.json
```

## How to scale up?

To scale up to terabytes of data, the input data can be chunked
("partitioned") into manageable slices that can typically fit in memory
on commodity machines.  Moreover, the data can be processed in parallel to
increase the throughput.

Because of the framework we use (Dask) and the way we have already structured
our code, this scaling up is built-in in our code, so we wouldn't have to
change anything in the core logic to make this work.

In terms of infrastructure, we would need to set up a Dask cluster and to
store the data in remote storage.  Partitioned Parquet datasets stored on,
e.g., AWS S3, could fit the bill.

## Continuous integration

Continuous integration has been set up on GitHub Actions.  See
`.github/workflows/main.yml`.
