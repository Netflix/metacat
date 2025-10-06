package com.netflix.metacat.connector.polaris.configs;

import com.netflix.metacat.connector.polaris.store.jdbc.PolarisDatabaseReplicaJDBC;
import com.netflix.metacat.connector.polaris.store.jdbc.PolarisTableReplicaJDBC;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;

/**
 * Configuration class for setting up the Polaris persistence reader.
 * This configuration is activated when the property 'spring.datasource.reader.url' is set.
 */
@Configuration
@ConditionalOnProperty(prefix = "spring.datasource.reader", name = "url")
@Slf4j
public class PolarisPersistenceReaderConfig {

    /**
     * Creates a DataSource bean for the reader using HikariCP connection pool.
     * The properties for the DataSource are prefixed with 'spring.datasource.reader.hikari'.
     *
     * @param readerDataSourceProperties the properties used to configure the DataSource.
     * @return a configured DataSource instance.
     */
    @Bean
    @ConfigurationProperties(prefix = "spring.datasource.reader.hikari")
    public DataSource readerDataSource(@Qualifier("readerDataSourceProperties")
                                           final DataSourceProperties readerDataSourceProperties) {
        return readerDataSourceProperties
            .initializeDataSourceBuilder()
            .type(HikariDataSource.class)
            .build();
    }

    /**
     * Creates a DataSourceProperties bean for the reader.
     * The properties are prefixed with 'spring.datasource.reader'.
     *
     * @return a DataSourceProperties instance.
     */
    @Bean
    @ConfigurationProperties("spring.datasource.reader")
    public DataSourceProperties readerDataSourceProperties() {
        return new DataSourceProperties();
    }

    /**
     * Creates a JdbcTemplate bean for the reader, using the configured DataSource.
     *
     * @param dataSource the DataSource to be used by the JdbcTemplate.
     * @return a JdbcTemplate instance.
     */
    @Bean
    public JdbcTemplate readerJdbcTemplate(@Qualifier("readerDataSource") final DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * Creates a PolarisDatabaseReplicaJDBC.
     *
     * @param readerJdbcTemplate the readerJdbcTemplate
     * @return a JdbcTemplate instance.
     */
    @Bean
    public PolarisDatabaseReplicaJDBC polarisDatabaseReplicaJDBC(final JdbcTemplate readerJdbcTemplate) {
        return new PolarisDatabaseReplicaJDBC(readerJdbcTemplate);
    }

    /**
     * Creates a polarisTableReplicaJDBC.
     *
     * @param readerJdbcTemplate the readerJdbcTemplate
     * @return a JdbcTemplate instance.
     */
    @Bean
    public PolarisTableReplicaJDBC polarisTableReplicaJDBC(final JdbcTemplate readerJdbcTemplate) {
        return new PolarisTableReplicaJDBC(readerJdbcTemplate);
    }
}
