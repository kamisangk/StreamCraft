# StreamCraft

StreamCraft is a visual stream-processing workbench for Apache Flink. It provides a Spring Boot management service, a browser-based DAG studio, and a Flink runtime JAR that converts saved pipeline definitions into executable Flink jobs.

## Repository Layout

```text
StreamCraft/
  core/        Flink runtime entrypoint, connector factories, transforms, and shared parser code
  service/     Spring Boot web application, REST APIs, Thymeleaf pages, and static frontend assets
  streamcraft-dist/
               Binary distribution module with assembly descriptor, scripts, config, and empty data/log directories
  docs/        Design notes and implementation plans
```

The root `pom.xml` builds `core`, `service`, and `streamcraft-dist`. The distribution module uses `streamcraft-dist/src/main/assembly/bin.xml` to assemble the deployable package. Shared validation and connector parser code lives under `core/src/shared/java` and is compiled into both runtime and service modules.


## Screenshots

<img width="2285" height="681" alt="monitor" src="https://github.com/user-attachments/assets/7b2c32e1-0651-4a64-9482-0beb0d603733" />
<img width="2540" height="1333" alt="DAG" src="https://github.com/user-attachments/assets/15a58e74-e5eb-45d1-ba1d-695daf436078" />
<img width="2281" height="1037" alt="monitor" src="https://github.com/user-attachments/assets/974dea64-68c1-4a6f-b6bb-7fc11b9cf811" />
<img width="2267" height="1333" alt="DAG" src="https://github.com/user-attachments/assets/10f8a9cb-8a7f-442b-9527-8a526636f4c2" />


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
| `PUT`, `PRUNE`, `RENAME` | Add, remove, and rename fields |
| `DESERIALIZE`, `SERIALIZE` | Parse and serialize record payloads |
| `FILTER`, `ROUTE`, `CASE_WHEN` | Filter records, split branches, and derive conditional values |
| `CAST`, `EVAL`, `GROK`, `CUSTOM_CODE` | Convert types, evaluate expressions, extract patterns, and run custom Java logic |
| `FLATTEN`, `EXPLODE` | Flatten nested objects and expand arrays into multiple records |
| `DEDUPLICATE` | Deduplicate by key with TTL or window controls |
| `LOOKUP_ENRICH`, `LOOKUP_JOIN` | Enrich records through static lookup data and lookup joins |
| `STREAM_JOIN` | Join two upstream streams through explicit left and right input ports |
| `DATA_QUALITY` | Validate required fields, types, ranges, enums, and regular expressions |
| `TIME_DERIVE` | Parse, format, convert, and derive time partition fields |
| `MASK_HASH` | Mask sensitive values or hash them before writing downstream |
| `AGGREGATE` | Aggregate over count or time windows with count, sum, min, max, avg, count distinct, first/last value, top N, collect list, and collect set |

### Sinks

| Operator | Purpose |
|---|---|
| `KAFKA_SINK` | Write records to Kafka |
| `JDBC_SINK` | Write records to relational databases |
| `ELASTICSEARCH_SINK` | Write documents to Elasticsearch |
| `INFLUXDB_SINK` | Write points to InfluxDB |
| `HDFS_FILE_SINK` | Write files to HDFS |

## Requirements

- Java 17
- Maven 3.6+

Build all modules and create the binary package from the repository root:

```bash
mvn clean package -DskipTests
```

Expected package artifacts:

```text
streamcraft-dist/target/streamcraft-0.0.1-SNAPSHOT-bin.tar.gz
streamcraft-dist/target/streamcraft-0.0.1-SNAPSHOT-bin.zip
```

Run the core test suite:

```bash
mvn -pl core test
```

Compile the service and tests without running service tests:

```bash
mvn -pl service -DskipTests test-compile
```

## Binary Package

The assembled package layout is:

```text
streamcraft-<version>-bin/
  bin/
    start-service.sh
    stop-service.sh
    status-service.sh
    streamcraft-env.sh
    start-service.bat
    stop-service.bat
  conf/
    application.properties
  libs/
    streamcraft-service-<version>.jar
    *.jar
  flink-libs/
    streamcraft-core.jar
  logs/
  data/
  docs/
    README.md
    README_CN.md
```

## Configuration

Common properties:

| Property | Default | Description |
|---|---|---|
| `server.port` | `8080` | HTTP port |
| `streamcraft.datasource.type` | `sqlite` | Database type: `sqlite` or `mysql` |
| `spring.datasource.url` | `jdbc:sqlite:streamcraft-service.db` | Database connection URL |
| `spring.jpa.hibernate.ddl-auto` | `update` |  |
| `spring.datasource.hikari.maximum-pool-size` | `1` | Database pool size |
| `streamcraft.auth.remember-me-validity-seconds` | `1209600` | Remember-me cookie validity in seconds |
| `streamcraft.internal.token` | `streamcraft-local-internal-token` | Token for protected internal service calls |
| `logging.file.name` | `./logs/streamcraft-service.log` | Log file path |
| `logging.level.root` | `INFO` | Log level |
| `streamcraft.flink.core-jar-path` | `../core/target/streamcraft-core.jar` | Core JAR path |
| `streamcraft.flink.connect-timeout` | `2s` | Flink REST connection timeout |
| `streamcraft.flink.read-timeout` | `3s` | Flink REST read timeout |
| `streamcraft.runtime-target.validation-interval` | `5000` | Runtime target health-check interval in milliseconds |
| `streamcraft.pipeline.runtime.service-base-url` | `http://localhost:8080` | Base URL used by Flink jobs to call the service |
| `streamcraft.pipeline.runtime.parallelism` | `1` | Default pipeline parallelism |

Use MySQL by setting the datasource type and URL:

```bash
export STREAMCRAFT_DATASOURCE_TYPE=mysql
export STREAMCRAFT_DATASOURCE_URL='jdbc:mysql://localhost:3306/streamcraft?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC'
export SPRING_DATASOURCE_USERNAME=streamcraft
export SPRING_DATASOURCE_PASSWORD=streamcraft
bin/start-service.sh
```

## Main Pages

| Path | Page |
|---|---|
| `/login` | Login |
| `/main` | Overview |
| `/runtime-target` | Flink runtime target |
| `/pipelines` | pipeline list |
| `/pipelines/{id}/monitor` | pipeline monitor detail |
| `/studio` | Create pipeline |
| `/studio/{id}` | Edit pipeline |
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
