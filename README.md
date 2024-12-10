# sqlmesh demo

This demo servers as basic replication of `matsonj/stocks` demo which is built on dbt.

The goal is to extend the project to SQLMesh and show of some nice features.

## Getting started

1. clone the repo.
2. setup your python environment with `uv` - if not installed, check out the [uv docs](https://docs.astral.sh/uv/getting-started/installation/)
  1. run `uv venv`
  2. run `source venv/bin/activate`
  3. run `uv pip install -r pyproject.toml`
3. add your motherduck db & token to in `.dlt/secrets.toml` - see [more here](https://dlthub.com/docs/dlt-ecosystem/destinations/motherduck#setup-guide).
4. run `python3 stock_data_pipeline.py` to hydrate the data.
5. run `sqlmesh info` to make sure everything checks out.
 - if it does not find your md token, make sure to `source` the token as MOTHERDUCK_TOKEN (Web UI also works).
6. run `sqlmesh plan` to execute your first run (type `y` to push the data in).

