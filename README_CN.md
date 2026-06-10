# StreamCraft

StreamCraft 是面向 Apache Flink 的可视化流处理工作台。它由 Spring Boot 管理服务、浏览器 DAG Studio，以及负责把已保存pipeline定义转换为 Flink 作业的运行时 JAR 组成。

## 仓库结构

```text
StreamCraft/
  core/        Flink 运行入口、连接器工厂、转换算子和共享解析代码
  service/     Spring Boot Web 应用、REST API、Thymeleaf 页面和静态前端资源
  streamcraft-dist/
               二进制发行包模块，包含 assembly 描述、启动脚本、配置文件和空 data/logs 目录
  docs/        设计文档和实施计划
```

根目录 `pom.xml` 负责构建 `core`、`service` 和 `streamcraft-dist`。发行包模块通过 `streamcraft-dist/src/main/assembly/bin.xml` 组装可部署包。共享校验和连接器配置解析代码位于 `core/src/shared/java`，会同时编译进运行时和服务模块。

## 支持的算子

### Source

| 算子 | 用途 |
|---|---|
| `KAFKA_SOURCE` | 消费 Kafka 记录 |
| `JDBC_SOURCE` | 以全量或增量模式读取关系型数据库数据 |
| `ELASTICSEARCH_SOURCE` | 以全量或增量模式读取 Elasticsearch 文档 |
| `INFLUXDB_SOURCE` | 读取 InfluxDB 时序数据 |
| `HDFS_FILE_SOURCE` | 从 HDFS 读取文件 |

### Transform

| 算子 | 用途 |
|---|---|
| `PUT`, `PRUNE`, `RENAME` | 新增、删除和重命名字段 |
| `DESERIALIZE`, `SERIALIZE` | 解析和序列化记录内容 |
| `FILTER`, `ROUTE`, `CASE_WHEN` | 过滤记录、条件分支和条件派生字段 |
| `CAST`, `EVAL`, `GROK`, `CUSTOM_CODE` | 类型转换、表达式计算、模式提取和自定义 Java 逻辑 |
| `FLATTEN`, `EXPLODE` | 打平嵌套对象，并把数组展开为多条记录 |
| `DEDUPLICATE` | 按 key 结合 TTL 或窗口进行去重 |
| `LOOKUP_ENRICH`, `LOOKUP_JOIN` | 通过静态维表数据补全记录或执行查找关联 |
| `STREAM_JOIN` | 通过显式 left/right 输入端口关联两路上游流 |
| `DATA_QUALITY` | 校验必填、类型、范围、枚举和正则规则 |
| `TIME_DERIVE` | 解析、格式化、转换和派生时间分区字段 |
| `MASK_HASH` | 在写入下游前对敏感值做脱敏或哈希处理 |
| `AGGREGATE` | 在计数窗口或时间窗口上执行 count、sum、min、max、avg、count distinct、first/last value、top N、collect list 和 collect set |

### Sink

| 算子 | 用途 |
|---|---|
| `KAFKA_SINK` | 写入 Kafka |
| `JDBC_SINK` | 写入关系型数据库 |
| `ELASTICSEARCH_SINK` | 写入 Elasticsearch 文档 |
| `INFLUXDB_SINK` | 写入 InfluxDB 点数据 |
| `HDFS_FILE_SINK` | 写入 HDFS 文件 |

## 环境要求

- Java 17
- Maven 3.6+

在仓库根目录构建全部模块并生成二进制部署包：

```bash
mvn clean package -DskipTests
```

预期打包产物：

```text
streamcraft-dist/target/streamcraft-0.0.1-SNAPSHOT-bin.tar.gz
streamcraft-dist/target/streamcraft-0.0.1-SNAPSHOT-bin.zip
```

运行 core 测试：

```bash
mvn -pl core test
```

只编译 service 和测试代码，不运行 service 测试：

```bash
mvn -pl service -DskipTests test-compile
```

## 二进制部署包

组装后的部署包结构如下：

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

## 配置

常用配置项：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `server.port` | `8080` | HTTP 端口 |
| `streamcraft.datasource.type` | `sqlite` | 数据库类型：`sqlite` 或 `mysql` |
| `spring.datasource.url` | `jdbc:sqlite:streamcraft-service.db` | 数据库连接 URL |
| `spring.jpa.hibernate.ddl-auto` | `update` |  |
| `spring.datasource.hikari.maximum-pool-size` | `1` | 数据库连接池大小 |
| `streamcraft.auth.remember-me-validity-seconds` | `1209600` | 记住登录状态 Cookie 的有效期，单位秒 |
| `streamcraft.internal.token` | `streamcraft-local-internal-token` | 受保护内部服务调用的 token |
| `logging.file.name` | `./logs/streamcraft-service.log` | 日志文件路径 |
| `logging.level.root` | `INFO` | 日志级别 |
| `streamcraft.flink.core-jar-path` | `../core/target/streamcraft-core.jar` | Core JAR 路径 |
| `streamcraft.flink.connect-timeout` | `2s` | Flink REST 连接超时时间 |
| `streamcraft.flink.read-timeout` | `3s` | Flink REST 读取超时时间 |
| `streamcraft.runtime-target.validation-interval` | `5000` | 运行目标健康检查间隔，单位毫秒 |
| `streamcraft.pipeline.runtime.service-base-url` | `http://localhost:8080` | Flink 作业访问 Service 时使用的基础 URL |
| `streamcraft.pipeline.runtime.parallelism` | `1` | 默认pipeline并行度 |

使用 MySQL 时设置数据库类型和连接地址：

```bash
export STREAMCRAFT_DATASOURCE_TYPE=mysql
export STREAMCRAFT_DATASOURCE_URL='jdbc:mysql://localhost:3306/streamcraft?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC'
export SPRING_DATASOURCE_USERNAME=streamcraft
export SPRING_DATASOURCE_PASSWORD=streamcraft
bin/start-service.sh
```

## 主要页面

| 路径 | 页面 |
|---|---|
| `/login` | 登录 |
| `/main` | 概览 |
| `/runtime-target` | Flink 运行目标 |
| `/pipelines` | pipeline列表 |
| `/pipelines/{id}/monitor` | pipeline监控详情 |
| `/studio` | 新建pipeline |
| `/studio/{id}` | 编辑pipeline |
| `/settings` | 账号设置 |

## 主要 API

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/api/pipelines` | 保存pipeline |
| `GET` | `/api/pipelines` | 查询pipeline列表 |
| `GET` | `/api/pipelines/{id}` | 查询pipeline详情 |
| `GET` | `/api/pipelines/{id}/definition` | 查询运行时pipeline定义 |
| `POST` | `/api/pipelines/preview` | 预览pipeline |
| `POST` | `/api/pipelines/{id}/run` | 运行pipeline |
| `POST` | `/api/pipelines/{id}/stop` | 停止pipeline |
| `DELETE` | `/api/pipelines/{id}` | 删除pipeline |
| `GET` | `/api/pipelines/{id}/metrics` | 查询 Flink 指标 |
| `GET` | `/api/pipelines/monitor` | 查询全局任务监控数据 |
| `GET` | `/api/overview` | 查询概览统计 |
| `GET` | `/api/runtime-target` | 查询 Flink 目标 |
| `PUT` | `/api/runtime-target/standalone` | 保存 Flink 目标 |
| `POST` | `/api/settings/password` | 修改管理员密码 |
