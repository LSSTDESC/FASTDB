# FASTDB
Development of the Fast Access to Survey Transients Database (FASTDB).

TODO: get this set up on readthedocs

In the mean time, to read the documentation, check out this git archive.  Make sure you have [Sphinx](https://www.sphinx-doc.org/en/master/index.html) installed on your system.  cd into the `docs` subdirectory of your checkout and run
```
make html
```

Then, point your web browser at:
```
file:///path/to/checkout/docs/_build/html/index.html
```
replacing `/path/to/checkout` with the absolute path of your git checkout.

## Repository Structure

The top-level directories in this repository hold different pieces of the
project:

- **`admin`** – YAML files that describe how the service is deployed on Spin.
- **`client`** – the `fastdb_client.py` module used to access the API.
- **`db`** – PostgreSQL database migration scripts.
- **`docker`** – Docker definitions used for Spin and local tests.
- **`docs`** – Sphinx documentation sources.
- **`examples`** – Jupyter notebooks showing how to use FASTDB.
- **`notes`** – assorted notes for developers.
- **`share`** – source copies of `.avsc` files describing the alert schema.
- **`src`** – web server code, services, admin utilities and supporting
  libraries.
- **`tests`** – the pytest test suite.