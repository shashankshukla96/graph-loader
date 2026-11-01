# Security policy

Neo4j Graph Loader is an alpha; only the latest release receives security fixes.

## Reporting a vulnerability

Do not open a public issue for a security problem. Email
**shashankshukla96@gmail.com** with the subject `graph-loader security` and
include:

* the version (`graph-loader --version`) and how it was installed;
* what an attacker can do and under which conditions;
* the smallest steps or input that demonstrate it.

Do not send credentials or real data. This is a one-person project: reports are
read on a best-effort basis with no promised response time, and a fix is
published as a new release. You will be told when the fix is out; please wait for
it before disclosing details publicly.

## What the loader does with credentials

The loader reads the Neo4j password only from the `NEO4J_PASSWORD` environment
variable (there is no password flag) and passes it to the loader containers it
starts through their environment. Set it explicitly: if it is unset the command
falls back to the default `changeme`. The service tests found no password in the
CLI output. The tested
environment is a trusted one: the loader needs access to the Docker daemon to
start its containers, which is equivalent to administrative access to that host.
Do not run it against a Docker daemon you do not control.
