# sqlmesh demo

This demo servers as basic replication of `matsonj/stocks` demo which is built on dbt.

The goal is to extend the project to SQLMesh and show of some nice features.

## Getting started

1. clone the repo.
2. run `uv pip install -r pyproject.toml`
3. add your motherduck db & token to in `.dlt/secrets.toml`.
4. run `python3 stock_data_pipeline.py` to hydrate the data.
5. run `sqlmesh info` to make sure everything checks out.
6. run `sqlmesh plan` to execute your first run (type `y` to push the data in).

