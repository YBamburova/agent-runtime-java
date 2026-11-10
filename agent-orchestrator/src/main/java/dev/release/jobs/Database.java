package dev.release.jobs;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;

/** Owns a bounded established pool and established versioned migrations, not a custom pool/migrator. */
public final class Database implements AutoCloseable {
    private final HikariDataSource dataSource;
    private final String schema;

    public Database(String url, String user, String password, String schema, int poolSize) {
        if (url == null || !url.startsWith("jdbc:postgresql:")) throw new IllegalArgumentException("A PostgreSQL JDBC URL is required");
        if (schema == null || !schema.matches("[a-z][a-z0-9_]{0,62}")) throw new IllegalArgumentException("Invalid schema name");
        if (poolSize < 1 || poolSize > 64) throw new IllegalArgumentException("Pool size must be 1..64");
        try { java.sql.DriverManager.getDriver(url); }
        catch (java.sql.SQLException e) { throw new IllegalStateException("PostgreSQL JDBC driver unavailable; build with -Ppostgres"); }
        this.schema = schema;
        var config = new HikariConfig();
        config.setJdbcUrl(url); config.setUsername(user); config.setPassword(password); config.setSchema(schema);
        config.setMaximumPoolSize(poolSize); config.setMinimumIdle(0);
        config.setConnectionTimeout(1_000); config.setValidationTimeout(1_000);
        config.setInitializationFailTimeout(-1);
        config.addDataSourceProperty("connectTimeout", "3");
        config.addDataSourceProperty("socketTimeout", "5");
        config.addDataSourceProperty("loginTimeout", "3");
        config.setPoolName("investigation-db");
        dataSource = new HikariDataSource(config);
    }
    public static Database fromEnvironment() {
        return new Database(System.getenv().getOrDefault("RELEASE_DB_URL", "jdbc:postgresql://localhost:55432/investigator"),
                System.getenv().getOrDefault("RELEASE_DB_USER", "investigator"),
                System.getenv().getOrDefault("RELEASE_DB_PASSWORD", "local-demo-only"),
                System.getenv().getOrDefault("RELEASE_DB_SCHEMA", "release_investigator"), 8);
    }
    public void migrate() {
        Flyway.configure().dataSource(dataSource).schemas(schema).defaultSchema(schema)
                .locations("classpath:db/migration").cleanDisabled(true).validateMigrationNaming(true).load().migrate();
    }
    public HikariDataSource dataSource() { return dataSource; }
    public PostgresJobStore store() { return new PostgresJobStore(dataSource); }
    @Override public void close() { dataSource.close(); }
}
