package com.streamcraft.service.distribution;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class DistributionPackageContractTest {

    private static final Path ROOT = Path.of("..").normalize();

    @Test
    void rootPomBuildsCoreServiceAndDistributionModule() throws Exception {
        String pom = Files.readString(ROOT.resolve("pom.xml"));

        assertThat(pom)
                .contains("<packaging>pom</packaging>")
                .contains("<module>core</module>")
                .contains("<module>service</module>")
                .contains("<module>streamcraft-dist</module>")
                .doesNotContain("<module>distribution</module>")
                .doesNotContain("<descriptor>assembly/bin.xml</descriptor>");

        assertThat(Files.exists(ROOT.resolve("distribution/pom.xml"))).isFalse();
        assertThat(Files.exists(ROOT.resolve("assembly/bin.xml"))).isFalse();
        assertThat(Files.exists(ROOT.resolve("package/bin"))).isFalse();
    }

    @Test
    void distributionAssemblyCreatesApacheStyleBinaryLayout() throws Exception {
        String servicePom = Files.readString(ROOT.resolve("service/pom.xml"));
        String distPom = Files.readString(ROOT.resolve("streamcraft-dist/pom.xml"));
        String assembly = Files.readString(ROOT.resolve("streamcraft-dist/src/main/assembly/bin.xml"));

        assertThat(servicePom)
                .contains("<artifactId>mysql-connector-j</artifactId>")
                .contains("<version>8.0.33</version>")
                .contains("<artifactId>maven-dependency-plugin</artifactId>")
                .contains("<includeScope>runtime</includeScope>")
                .contains("<outputDirectory>${project.build.directory}/libs</outputDirectory>")
                .contains("<excludeArtifactIds>spring-boot-devtools,spring-boot-configuration-processor</excludeArtifactIds>")
                .contains("<skip>true</skip>");

        assertThat(distPom)
                .contains("<artifactId>streamcraft-dist</artifactId>")
                .contains("<artifactId>maven-assembly-plugin</artifactId>")
                .contains("<descriptor>src/main/assembly/bin.xml</descriptor>");

        assertThat(assembly)
                .contains("<format>tar.gz</format>")
                .contains("<format>zip</format>")
                .contains("<outputDirectory>/bin</outputDirectory>")
                .contains("<outputDirectory>/conf</outputDirectory>")
                .contains("<outputDirectory>/libs</outputDirectory>")
                .contains("<outputDirectory>/flink-libs</outputDirectory>")
                .contains("<outputDirectory>/docs</outputDirectory>")
                .contains("<outputDirectory>/logs</outputDirectory>")
                .contains("<outputDirectory>/data</outputDirectory>")
                .contains("<directory>${project.parent.basedir}/service/target/libs</directory>")
                .contains("<directory>${project.basedir}/src/main/bin</directory>")
                .contains("<directory>${project.basedir}/src/main/conf</directory>")
                .contains("streamcraft-service-${project.version}.jar")
                .contains("streamcraft-core.jar")
                .doesNotContain("<outputDirectory>/lib</outputDirectory>");
    }

    @Test
    void binaryPackageHasLaunchScriptsAndDeployConfiguration() throws Exception {
        Path distMain = ROOT.resolve("streamcraft-dist/src/main");

        assertThat(Files.exists(distMain.resolve("bin/start-service.sh"))).isTrue();
        assertThat(Files.exists(distMain.resolve("bin/stop-service.sh"))).isTrue();
        assertThat(Files.exists(distMain.resolve("bin/status-service.sh"))).isTrue();
        assertThat(Files.exists(distMain.resolve("bin/streamcraft-env.sh"))).isTrue();
        assertThat(Files.exists(distMain.resolve("bin/start-service.bat"))).isTrue();
        assertThat(Files.exists(distMain.resolve("bin/stop-service.bat"))).isTrue();

        String startScript = Files.readString(distMain.resolve("bin/start-service.sh"));
        assertThat(startScript)
                .contains("-cp")
                .contains("${STREAMCRAFT_CONF_DIR}:${STREAMCRAFT_HOME}/libs/*")
                .contains("${STREAMCRAFT_SERVICE_MAIN_CLASS}")
                .doesNotContain("-jar");

        String envScript = Files.readString(distMain.resolve("bin/streamcraft-env.sh"));
        assertThat(envScript)
                .contains("com.streamcraft.service.StreamCraftServiceApplication")
                .contains("${STREAMCRAFT_HOME}/flink-libs/streamcraft-core.jar");

        String config = Files.readString(distMain.resolve("conf/application.properties"));
        assertThat(config)
                .contains(
                        "spring.datasource.url=${STREAMCRAFT_DATASOURCE_URL:jdbc:sqlite:${STREAMCRAFT_DATA_DIR:./data}/streamcraft-service.db}")
                .contains("streamcraft.datasource.type=${STREAMCRAFT_DATASOURCE_TYPE:sqlite}")
                .doesNotContain("spring.datasource.driver-class-name=org.sqlite.JDBC")
                .doesNotContain("spring.jpa.database-platform=org.hibernate.community.dialect.SQLiteDialect")
                .contains(
                        "logging.file.name=${STREAMCRAFT_LOG_FILE:${STREAMCRAFT_LOG_DIR:./logs}/streamcraft-service.log}")
                .contains("logging.level.root=${STREAMCRAFT_LOG_LEVEL:INFO}")
                .contains("streamcraft.flink.core-jar-path=${STREAMCRAFT_CORE_JAR_PATH:./flink-libs/streamcraft-core.jar}")
                .contains("streamcraft.internal.header-name=${STREAMCRAFT_INTERNAL_HEADER_NAME:X-StreamCraft-Internal-Token}");
        assertThat(Files.exists(distMain.resolve("conf/application-mysql.properties"))).isFalse();
    }
}
