package net.trino.excel.it;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import io.airlift.log.Logger;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.TrinoContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExcelConnectorTest {

    private static final Logger log = Logger.get(ExcelConnectorTest.class);

    private static final String NGINX_URL = "nginx-server";
    private static final String NGINX_IMAGE = "nginx:alpine";
    private static final String TRINO_IMAGE = "trinodb/trino:475";
    private static final String TEST_ZIP_FILENAME = "excel_test.zip";
    private static final String CATALOG_NAME = "excel";
    private static final Network NETWORK = Network.newNetwork();
    private static final int NGINX_PORT = 80;

    private Connection connection;
    private Statement statement;
    private static Path catalogFile = null;

    @Container
    private static final GenericContainer<?> nginx = new GenericContainer<>(DockerImageName.parse(NGINX_IMAGE))
            .withExposedPorts(NGINX_PORT)
            .withNetwork(NETWORK)
            .withNetworkAliases(NGINX_URL)
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource(String.format("test-data/%s", TEST_ZIP_FILENAME), 0755),
                    String.format("/usr/share/nginx/html/%s", TEST_ZIP_FILENAME))
            .waitingFor(Wait.forHttp(String.format("/%s", TEST_ZIP_FILENAME)).forStatusCode(200));

    @Container
    private static final TrinoContainer trino = new TrinoContainer(DockerImageName.parse(TRINO_IMAGE))
            .withNetwork(NETWORK)
            .withCopyFileToContainer(MountableFile.forHostPath(findPluginJar()),
                    "/usr/lib/trino/plugin/excel/trino-excel-plugin.jar")
            .withCopyFileToContainer(
                    createFile(),
                    String.format("/etc/trino/catalog/%s.properties", CATALOG_NAME))
            .dependsOn(nginx);

    @BeforeAll
    void setupTrinoAndConnection() throws SQLException {
        await()
                .atMost(30, SECONDS)
                .pollInterval(1, SECONDS)
                .until(() -> {
                    try (Connection conn = DriverManager.getConnection(trino.getJdbcUrl(), "test", null);
                            Statement stmt = conn.createStatement();
                            ResultSet rs = stmt.executeQuery("SELECT 1")) {
                        return rs.next();
                    } catch (SQLException e) {
                        return false;
                    }
                });
        connection = DriverManager.getConnection(trino.getJdbcUrl(), "test", null);
        statement = connection.createStatement();
    }

    @AfterAll
    void tearDown() throws SQLException {
        if (statement != null) {
            statement.close();
        }
        if (connection != null) {
            connection.close();
        }
        if (catalogFile != null) {
            try {
                Files.deleteIfExists(catalogFile);
            } catch (IOException e) {
                log.error("Warning: Failed to delete temporary catalog file: %s - %s%n", catalogFile, e.getMessage());
            }
        }
    }

    @Test
    void testShowSchemas() throws SQLException {
        await()
                .atMost(30, SECONDS)
                .pollInterval(1, SECONDS)
                .until(() -> trino.isRunning() && trino.isHealthy());
        ResultSet rs = statement.executeQuery(String.format("SHOW SCHEMAS FROM %s", CATALOG_NAME));
        List<String> schemas = new ArrayList<>();
        while (rs.next()) {
            schemas.add(rs.getString(1).toLowerCase());
        }
        assertThat(schemas).contains("information_schema", "products", "sample", "users");
    }

    @Test
    void testShowTables() throws SQLException {
        await()
                .atMost(30, SECONDS)
                .pollInterval(1, SECONDS)
                .until(() -> trino.isRunning() && trino.isHealthy());
        ResultSet rs = statement.executeQuery(String.format("SHOW TABLES FROM %s.sample", CATALOG_NAME));
        List<String> tables = new ArrayList<>();
        while (rs.next()) {
            tables.add(rs.getString(1).toLowerCase());
        }
        assertThat(tables).containsExactlyInAnyOrder("products", "users", "orders");
    }

    @Test
    void testSelectDataFromXlsx() throws SQLException {
        await()
                .atMost(30, SECONDS)
                .pollInterval(1, SECONDS)
                .until(() -> trino.isRunning() && trino.isHealthy());
        ResultSet rs = statement.executeQuery(
                String.format("SELECT * FROM %s.sample.users ORDER BY UserID", CATALOG_NAME));

        assertThat(rs.next()).isTrue();

        assertThat(rs.getString("UserID")).isEqualTo("101");
        assertThat(rs.getString("Username")).isEqualTo("alice");
        assertThat(rs.getString("Email")).isEqualTo("alice@example.com");

        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("UserID")).isEqualTo("102");
        assertThat(rs.getString("Username")).isEqualTo("bob");
        assertThat(rs.getString("Email")).isEqualTo("bob@example.com");

        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("UserID")).isEqualTo("103");
        assertThat(rs.getString("Username")).isEqualTo("carol");
        assertThat(rs.getString("Email")).isEqualTo("carol@example.com");
    }

    @Test
    void testSelectDataFromXls() throws SQLException {
        await()
                .atMost(30, SECONDS)
                .pollInterval(1, SECONDS)
                .until(() -> trino.isRunning() && trino.isHealthy());
        ResultSet rs = statement.executeQuery(String.format(
                "SELECT * FROM %s.products.products ORDER BY ID", CATALOG_NAME));

        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("ID")).isEqualTo("1");
        assertThat(rs.getString("Name")).isEqualTo("Laptop");
        assertThat(rs.getString("Price")).isEqualTo("1200");

        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("ID")).isEqualTo("2");
        assertThat(rs.getString("Name")).isEqualTo("Phone");
        assertThat(rs.getString("Price")).isEqualTo("800");

        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("ID")).isEqualTo("3");
        assertThat(rs.getString("Name")).isEqualTo("Tablet");
        assertThat(rs.getString("Price")).isEqualTo("400");

        assertThat(rs.next()).isFalse();
    }

    @Test
    void testSelectFromEmptySheet() throws SQLException {
        await()
                .atMost(30, SECONDS)
                .pollInterval(1, SECONDS)
                .until(() -> trino.isRunning() && trino.isHealthy());
        ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s.sample.orders", CATALOG_NAME));
        assertThat(rs.next()).isFalse();
    }

    private static Path findPluginJar() {
        Path pluginDirPath = Paths.get("build", "libs");
        String jarPattern = "trino-excel-plugin-.*\\.jar";
        try (var stream = Files.list(pluginDirPath)) {
            return stream
                    .filter(path -> !Files.isDirectory(path))
                    .filter(path -> path.getFileName().toString().matches(jarPattern))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            String.format(
                                    "Plugin JAR matching pattern '%s' not found in directory '%s'. Build the project first (e.g., ./gradlew build or ./gradlew jar).",
                                    jarPattern, pluginDirPath.toAbsolutePath())));
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Could not search for plugin JAR in '%s'", pluginDirPath.toAbsolutePath()), e);
        }
    }

    private static MountableFile createFile() {
        try {
            String catalogPropertiesContent = String.format(
                    "connector.name=excel%nexcel.zip.url=%s",
                    String.format("http://%s:%d/%s", NGINX_URL, NGINX_PORT, TEST_ZIP_FILENAME));
            catalogFile = Files.createTempFile(CATALOG_NAME, ".properties");
            Files.writeString(catalogFile, catalogPropertiesContent, StandardCharsets.UTF_8);
            return MountableFile.forHostPath(catalogFile);
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }
}
