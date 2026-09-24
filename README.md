# service-processes

| Branch | Status |
|--------|-----------|
| develop | ![Build Status](https://codebuild.eu-central-1.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiR2RnY0VNbHRNdFRRMXFJdnp6UlVROUlNeXJ1b3c4Wm1jRVJmeU9NN24wVVY3M1ZmZmo3aTQ2YWVzaTJ0S3BUNWZjY3RBeUgrVTZkbGYzcW9vbXJCY3dvPSIsIml2UGFyYW1ldGVyU3BlYyI6ImZXeitFbnQrTnV0djBKZzEiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=develop) [![codecov](https://codecov.io/github/swissgeo/service-processes/graph/badge.svg?token=07AV3ADXP1)](https://codecov.io/github/swissgeo/service-processes) |
| main | ![Build Status](https://codebuild.eu-central-1.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiR2RnY0VNbHRNdFRRMXFJdnp6UlVROUlNeXJ1b3c4Wm1jRVJmeU9NN24wVVY3M1ZmZmo3aTQ2YWVzaTJ0S3BUNWZjY3RBeUgrVTZkbGYzcW9vbXJCY3dvPSIsIml2UGFyYW1ldGVyU3BlYyI6ImZXeitFbnQrTnV0djBKZzEiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=main) [![codecov](https://codecov.io/gh/swissgeo/service-processes/branch/main/graph/badge.svg)](https://app.codecov.io/gh/swissgeo/service-processes/tree/main) |

Service processes provides an API for managing and monitoring pipelines, decoupling consumers from Airflow's native API.

## Development

This service uses the [FastAPI](https://fastapi.tiangolo.com/) framework.

### Dependencies

Prerequisites on host for development and build:

- python version 3.14
- uv
- docker and docker compose

### Setup

To create and activate a virtual Python environment with all dependencies installed:

```bash
make setup
```

Then run OTEL dependencies

```bash
make docker-compose-up
```

Then run the server from a separate terminal

```bash
make serve
```

### Linting and Formatting

This project uses `ruff` as linter and formatter. It also uses `ty` as type checker.

To lint and type check use

```bash
make lint
```

To format

```bash
make format
```

### Pre-Commit Hooks

This project uses pre-commit hook to lint and type-check before committing. Pre-commits hooks can
either be bypassed entirely with the `--no-verify` option (`git commit --no-verify ...`), or
individually using the `SKIP` environment variable (`SKIP=lint git commit ...`).

### Updating Packages

All packages used in production are pinned to a major version. Automatically updating these packages
will use the latest minor (or patch) version available. Packages used for development, on the other
hand, are not pinned unless they need to be used with a specific version of a production package
(for example, boto3-stubs for boto3).

To update the packages to the latest minor/compatible versions, run:

```bash
uv sync --upgrade
```

To see what major/incompatible releases would be available, run:

```bash
uv pip list --outdated
```

To update packages to a new major release, run:

```bash
uv add "fastapi[standard]~=v0.141"
```

### Testing

This project uses `pytest` for testing, to start the tests enter

```bash
make test
```

## OpenAPI

FastAPI automatically generates an OpenAPI schema, so each path operation should define its request
parameters and responses using `pydantic` models to ensure they are properly documented. To view the
OpenAPI documentation, run:

```bash
make serve
```

And then open:

- [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs) for Swagger
- [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc) for Redoc

## Observability

The service supports OpenTelemetry logging, tracing, and metrics.

### Metrics

#### FastAPI auto-instrumentation metrics

The `FastAPIInstrumentor` (backed by `opentelemetry-instrumentation-asgi`) emits the following
metrics automatically for every HTTP request. The default semantic-convention mode (`DEFAULT`) uses
the **old** HTTP semconv attribute names.

| Metric name | Type | Unit | Description |
|---|---|---|---|
| `http.server.request.duration` | Histogram | `s` | Duration of inbound HTTP requests |
| `http.server.request.body.size` | Histogram | `By` | Size of HTTP request messages (compressed) |
| `http.server.response.body.size` | Histogram | `By` | Size of HTTP response messages (compressed) |
| `http.server.active_requests` | UpDownCounter | `{request}` | Number of currently in-flight HTTP requests |

Attributes attached to `http.server.request.duration`, `http.server.request.body.size`, and
`http.server.response.body.size`:

| Attribute | Example | Description |
|---|---|---|
| `url.scheme` | `http` | URL scheme |
| `network.protocol.version` | `1.1` | Network protocol version |
| `http.request.method` | `GET` | HTTP request method |
| `http.route` | `/` | HTTP route |
| `http.response.status_code` | `200` | HTTP response status code |


Attributes attached to `http.server.active_requests`:

| Attribute | Example |
|---|---|
| `http.request.method` | `GET` |
| `url.scheme` | `http` |

> [!NOTE]
> The metrics above are from the new semantic convention for HTTP. They need to be enabled by setting `OTEL_SEMCONV_STABILITY_OPT_IN=http` in your environment. Use
> `OTEL_SEMCONV_STABILITY_OPT_IN=http/dup` to emit both old and new metrics simultaneously
> during a migration.

In production deployments, telemetry can be exported using the configured OTLP exporters,
typically to an OpenTelemetry Collector or any OTLP-compatible observability platform. Only the OTLP
exporter is currently implemented by the application configuration layer.

By default, local development with the FastAPI dev server (make serve) runs with
OpenTelemetry disabled and uses standard Python console logging for a simpler and more
readable developer experience.

See [OpenTelemetry Python Instrumentation documentation](https://opentelemetry.io/docs/languages/python/instrumentation)
for more information about adding tracing and metrics inside the application code.

### Logging implementation

The application uses the OpenTelemetry `LoggerProvider` directly to export logs.

> [!WARNING]
> The deprecated `opentelemetry-instrumentation-logging` package is intentionally not
> used, as `LoggerProvider` already associates logs with the active trace/span context and
> provides native structured OTEL log exporting.

### Local OTEL testing

To test the full OTEL configuration locally (logs, traces, and metrics exported through
OpenTelemetry), use the provided OTEL environment configuration and run the application
with Docker:

1. Start the local otel collector

    ```bash
    make start-otel
    ```

2. In a new shell start the application

    ```bash
    cp .env.otel .env
    make dockerrun
    ```

This configuration enables the OTLP exporters and sends telemetry to the local configured
OpenTelemetry collector endpoint created via `make start-otel`.

Then you will see OTEL logs and metrics in the first shell in which you started `make start-otel` and
you can see the full trace using `jaeger` trace explorer at http://localhost:16686
