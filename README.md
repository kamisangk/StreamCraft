<p align="center">
  <img src="service/src/main/resources/static/brand-logo.png" alt="StreamCraft logo" width="88" height="88">
</p>

<h1 align="center">StreamCraft</h1>

<p align="center">
  A visual stream-processing workbench for Apache Flink
</p>

<p align="center">
  English | <a href="README_CN.md">简体中文</a>
</p>

---

## Overview

StreamCraft turns stream-processing jobs from hand-written code into diagrams: drag operators, connect them into a DAG, and configure parameters in the browser-based DAG Studio. Save the pipeline, submit it to a Flink cluster with one click, and watch job status and metrics on the built-in monitoring pages.

It consists of two parts:

- **service** —— Spring Boot management service providing REST APIs, Thymeleaf pages, and pipeline metadata storage
- **core** —— Flink runtime JAR that compiles saved pipeline definitions into executable Flink jobs

## Features

- **Visual DAG Studio**: drag-and-drop authoring, port-based connections, per-node configuration, context-menu copy/delete
- **30+ built-in operators**: covering Source / Transform / Sink layers, from field-level processing to windowed aggregation
- **Pipeline lifecycle management**: save, preview, run, stop, and delete in one place
- **Runtime monitoring**: job status, Flink metrics, and runtime data in real time
- **Database support**: SQLite out of the box, switchable to MySQL

## Screenshots

<p align="center">
  <img src=".github/docs/assets/monitor-light.png" alt="Task monitoring (light mode)" width="100%">
  <img src=".github/docs/assets/dag-light.png" alt="DAG Studio (light mode)" width="100%">
  <img src=".github/docs/assets/monitor-dark.png" alt="Task monitoring (dark mode)" width="100%">
  <img src=".github/docs/assets/dag-dark.png" alt="DAG Studio (dark mode)" width="100%">
</p>

## Getting Started

### Requirements

- Java 17
- Maven 3.6+
- (Optional) An Apache Flink standalone cluster for running jobs

### Build

Build all modules and generate the binary package from the repository root:

```bash
mvn clean package -DskipTests
```

Expected artifacts:

```text
streamcraft-dist/target/streamcraft-0.2.0-bin.tar.gz
streamcraft-dist/target/streamcraft-0.2.0-bin.zip
```

### Start

Extract the package and run the startup script (on Windows, `bin\start-service.bat` also works):

```bash
bin/start-service.sh
```

Then open <http://localhost:8080>.

### Run Tests

```bash
# full core test suite
mvn -pl core test

# compile service and its tests only
mvn -pl service -DskipTests test-compile
```

## Repository Layout

```text
StreamCraft/
  core/            Flink runtime entrypoint, connector factories, transforms, and shared parser code
  service/         Spring Boot web application, REST APIs, Thymeleaf pages, and static frontend assets
  streamcraft-dist/  Binary distribution module (assembly descriptor, startup scripts, config)
  .docs/           Architecture, API, and operator documentation
  .github/docs/assets/  README screenshot assets
```

The root `pom.xml` builds the `core`, `service`, and `streamcraft-dist` modules; shared validation and connector parser code lives under `core/src/shared/java` and is compiled into both the runtime and the service.

## Supported Operators

### Sources

| Operator | Purpose |
|---|---|
| `KAFKA_SOURCE` | Consume Kafka records |
| `JDBC_SOURCE` | Read relational data in full or incremental mode |
| `ELASTICSEARCH_SOURCE` | Read Elasticsearch documents in full or incremental mode |
| `INFLUXDB_SOURCE` | Read InfluxDB time-series data |
| `HDFS_FILE_SOURCE` | Read files from HDFS |

### Transforms

| Operator | Purpose |
|---|---|
| `PUT` / `PRUNE` / `RENAME` | Add, remove, and rename fields |
| `DESERIALIZE` / `SERIALIZE` | Parse and serialize record payloads |
| `FILTER` / `ROUTE` / `CASE_WHEN` | Filter records, split branches, and derive conditional values |
| `CAST` / `EVAL` / `GROK` / `CUSTOM_CODE` | Convert types, evaluate expressions, extract patterns, and run custom Java logic |
| `FLATTEN` / `EXPLODE` | Flatten nested objects and expand arrays into multiple records |
| `DEDUPLICATE` | Deduplicate by key with TTL or window controls |
| `LOOKUP_ENRICH` / `LOOKUP_JOIN` | Enrich records through static lookup data and lookup joins |
| `STREAM_JOIN` | Join two upstream streams through explicit left/right input ports |
| `DATA_QUALITY` | Validate required fields, types, ranges, enums, and regular expressions |
| `TIME_DERIVE` | Parse, format, convert, and derive time partition fields |
| `MASK_HASH` | Mask sensitive values or hash them before writing downstream |
| `AGGREGATE` | Count/time windows with count, sum, min, max, avg, count distinct, first/last, top N, collect list/set |

### Sinks

| Operator | Purpose |
|---|---|
| `KAFKA_SINK` | Write records to Kafka |
| `JDBC_SINK` | Write records to relational databases |
| `ELASTICSEARCH_SINK` | Write documents to Elasticsearch |
| `INFLUXDB_SINK` | Write points to InfluxDB |
| `HDFS_FILE_SINK` | Write files to HDFS |

> Pipeline edges use semantic ports: ordinary operators use `records`; Filter uses `matched` / `rejected`; Data Quality uses `clean` / `dirty`; Stream Join uses `left` / `right`; Route output ports are defined by its route configuration.

## Configuration

Common properties (`conf/application.properties`):

| Property | Default | Description |
|---|---|---|
| `server.port` | `8080` | HTTP port (overridable via the `SERVER_PORT` environment variable) |
| `streamcraft.datasource.type` | `sqlite` | Database type: `sqlite` or `mysql` |
| `spring.datasource.url` | `jdbc:sqlite:streamcraft-service.db` | Database connection URL |
| `spring.jpa.hibernate.ddl-auto` | `validate` | Flyway owns the schema; Hibernate validates only |
| `spring.datasource.hikari.maximum-pool-size` | `1` | Database pool size |
| `streamcraft.auth.remember-me-validity-seconds` | `1209600` | Remember-me cookie validity in seconds |
| `streamcraft.internal.token` | `streamcraft-local-internal-token` | Token for protected internal service calls |
| `logging.file.name` | `./logs/streamcraft-service.log` | Log file path |
| `streamcraft.flink.core-jar-path` | `../core/target/streamcraft-core.jar` | Core JAR path |
| `streamcraft.flink.connect-timeout` | `2s` | Flink REST connection timeout |
| `streamcraft.flink.read-timeout` | `3s` | Flink REST read timeout |
| `streamcraft.runtime-target.validation-interval` | `5000` | Runtime target health-check interval in milliseconds |
| `streamcraft.pipeline.runtime.service-base-url` | `http://localhost:8080` | Base URL used by Flink jobs to call the service |
| `streamcraft.pipeline.runtime.parallelism` | `1` | Default pipeline parallelism |

### Switching to MySQL

```bash
export STREAMCRAFT_DATASOURCE_TYPE=mysql
export STREAMCRAFT_DATASOURCE_URL='jdbc:mysql://localhost:3306/streamcraft?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC'
export SPRING_DATASOURCE_USERNAME=streamcraft
export SPRING_DATASOURCE_PASSWORD=streamcraft
bin/start-service.sh
```

Database schemas are managed by Flyway migrations: a fresh database receives the initial migration, while an existing database is baselined first. **Back up your database before upgrading.**

## Package Layout

```text
streamcraft-<version>-bin/
  bin/            start/stop/status scripts (sh and bat) plus streamcraft-env.sh
  conf/           application.properties
  libs/           streamcraft-service-<version>.jar and dependencies
  flink-libs/     streamcraft-core.jar
  logs/           runtime logs
  data/           database files
  docs/           README.md / README_CN.md
```

## Main Pages

| Path | Page |
|---|---|
| `/login` | Login |
| `/main` | Overview |
| `/pipelines` | Pipeline list |
| `/pipelines/{id}/monitor` | Pipeline monitor detail |
| `/studio` | Create pipeline |
| `/studio/{id}` | Edit pipeline |
| `/runtime-target` | Flink runtime target |
| `/settings` | Account settings |

## Main APIs

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/pipelines` | Save pipeline |
| `GET` | `/api/pipelines` | List pipelines |
| `GET` | `/api/pipelines/{id}` | Read pipeline details |
| `GET` | `/api/pipelines/{id}/definition` | Read runtime pipeline definition |
| `POST` | `/api/pipelines/preview` | Preview pipeline |
| `POST` | `/api/pipelines/{id}/run` | Run pipeline |
| `POST` | `/api/pipelines/{id}/stop` | Stop pipeline |
| `DELETE` | `/api/pipelines/{id}` | Delete pipeline |
| `GET` | `/api/pipelines/{id}/metrics` | Read Flink metrics |
| `GET` | `/api/pipelines/monitor` | Read global task monitor data |
| `GET` | `/api/overview` | Read overview statistics |
| `GET` | `/api/runtime-target` | Read the Flink target |
| `PUT` | `/api/runtime-target/standalone` | Save the Flink target |
| `POST` | `/api/settings/password` | Change the admin password |

## License

[Apache License 2.0](LICENSE)
