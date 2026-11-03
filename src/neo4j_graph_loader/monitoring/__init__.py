"""Portable monitoring assets (E15.F2.S10): the Prometheus rules, Grafana dashboards and scrape configuration of
``monitoring/`` in the checkout, shipped inside the package so a pip-installed ``graph-loader`` can write them for the
user's own Prometheus and Grafana (``graph-loader monitoring export``, see :mod:`.export`).

The directory tree below this package equals the checkout's ``monitoring/`` byte for byte (a test enforces it)."""
