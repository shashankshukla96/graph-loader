# Support, security and reporting problems

Neo4j Graph Loader is an alpha maintained by one person on a best-effort basis.
There is no service-level commitment and no promised response time. Only the
latest alpha release receives fixes.

* **Maintainer:** Shashank Shukla
* **Questions, bugs, feature requests:** <https://github.com/shashankshukla96/graph-loader/issues>
* **Security problems (private):** shashankshukla96@gmail.com, subject
  `graph-loader security`. See [SECURITY.md](../SECURITY.md).
* **License:** Apache License 2.0 ([LICENSE](../LICENSE)).

What is and is not supported (environment, tested versions, limits) is stated in
the README section [Support and limits](../README.md#support-and-limits). Check
it first: a problem outside the listed environment, in an internal module, or in
`execution.mode: native_disjoint` is not a supported-behavior bug.

## Before you report

1. Run `graph-loader --version` and make sure you are on the latest release.
2. Validate your schema without services: `graph-loader validate-schema --config your-schema.yaml`.
3. Read the matching section of [Troubleshooting](USAGE.md#11-troubleshooting) and,
   for a bulk run, the `stage=` lines in the CLI output ([CLI reference](CLI.md)).

## Minimal reproduction template

A report with a small schema and a handful of records that anyone can run is
fixed far faster than a description. Copy this into the issue and fill it in.

````markdown
**What I did** (the exact `graph-loader` command, or the Python call):

```text
graph-loader start --mode bulk --config schema.yaml
```

**What I expected:**

**What happened** (error text and the `stage=` lines around it, trimmed to the relevant part):

```text
```

**Environment**

- graph-loader version (`graph-loader --version`):
- Python version and operating system / architecture:
- Neo4j version and edition:
- Kafka version (broker count, security protocol):
- Docker Engine version (if `start` was used):
- Installed from: PyPI / TestPyPI / other:

**Schema** (the smallest schema that still shows the problem; rename labels and topics if they are sensitive):

```yaml
```

**Records** (3-10 JSON records per topic that trigger it, or a producer snippet that creates them):

```json
```

**Steps to reproduce from an empty Neo4j database and empty topics:**

1.
2.

**Already tried** (other mode flag, a fresh consumer group id, a newer release):
````

Never include passwords, tokens, connection strings with credentials, private
hostnames or addresses, or real personal data. Remove them from logs before you
paste. If you cannot reproduce without sensitive data, say so and describe the
data's shape instead.

## Contributing

Bug reports with a reproduction are the most useful contribution. For a code or
documentation change, open an issue first so the approach can be agreed, then
send a pull request. Working on the library itself needs a source checkout, not
the PyPI package: see [Distribution development](DISTRIBUTION_DEVELOPMENT.md) for
the environment, `make test-unit` for the unit tests and `make lint` for the lint
baseline. Changes must keep the unit tests and lint green and update the
documentation they touch. By submitting a contribution you agree that it is
licensed under the Apache License 2.0, the license of the project.
