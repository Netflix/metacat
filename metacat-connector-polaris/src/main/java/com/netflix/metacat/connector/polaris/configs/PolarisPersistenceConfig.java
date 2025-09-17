package com.netflix.metacat.connector.polaris.configs;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;

/**
 * Configuration class for setting up persistence-related beans and properties for the Polaris connector.
 * This configuration includes entity scanning, JPA repository enabling, transaction management, and
 * data source configuration using HikariCP.
 */
@Configuration
@EntityScan("com.netflix.metacat.connector.polaris.store.entities")
@EnableJpaRepositories(
    basePackages = "com.netflix.metacat.connector.polaris.store.repos"
)
@EnableJpaAuditing
@EnableTransactionManagement(proxyTargetClass = true)
@ImportAutoConfiguration({DataSourceAutoConfiguration.class,
    DataSourceTransactionManagerAutoConfiguration.class, HibernateJpaAutoConfiguration.class,
    TransactionAutoConfiguration.class})
public class PolarisPersistenceConfig {

    /**
     * Configures the primary data source bean using HikariCP for connection pooling.
     * The data source properties are prefixed with "spring.datasource.hikari" in the application configuration.
     *
     * @param dataSourceProperties the data source properties used to configure the data source.
     * @return a configured DataSource instance.
     */
    @Bean
    @Primary
    @ConfigurationProperties(prefix = "spring.datasource.hikari")
    public DataSource dataSource(final DataSourceProperties dataSourceProperties) {
        return dataSourceProperties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    /**
     * Configures the data source properties bean. This bean holds the configuration properties
     * for the data source and is used to initialize the data source.
     * The properties are prefixed with "spring.datasource" in the application configuration.
     *
     * @return a configured DataSourceProperties instance.
     */
    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource")
    public DataSourceProperties dataSourceProperties() {
        return new DataSourceProperties();
    }
}
