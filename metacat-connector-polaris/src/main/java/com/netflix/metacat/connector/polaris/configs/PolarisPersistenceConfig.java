package com.netflix.metacat.connector.polaris.configs;

import com.netflix.metacat.connector.polaris.store.PolarisStoreConnector;
import com.netflix.metacat.connector.polaris.store.PolarisStoreService;
import com.netflix.metacat.connector.polaris.store.repos.PolarisDatabaseRepository;
import com.netflix.metacat.connector.polaris.store.repos.PolarisTableRepository;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import javax.sql.DataSource;

/**
 * The Polaris Store Persistence config.
 *
 */
@Configuration
@EntityScan("com.netflix.metacat.connector.polaris.store.entities")
@EnableJpaRepositories("com.netflix.metacat.connector.polaris.store.repos")
@EnableJpaAuditing
@EnableTransactionManagement(proxyTargetClass = true)
@ImportAutoConfiguration({DataSourceAutoConfiguration.class,
    DataSourceTransactionManagerAutoConfiguration.class, HibernateJpaAutoConfiguration.class,
    TransactionAutoConfiguration.class})
public class PolarisPersistenceConfig {

    /**
     * Primary datasource. Since connectors can have data sources configured, polaris store JPA needs to be
     * explicitly configured.
     *
     * @param dataSourceProperties datasource properties
     * @return Datasource
     */
    @Bean
    @ConfigurationProperties(prefix = "spring.datasource.hikari")
    public DataSource dataSource(final DataSourceProperties dataSourceProperties) {
        return dataSourceProperties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    /**
     * Datasource properties.
     *
     * @return DataSourceProperties
     */
    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource")
    public DataSourceProperties dataSourceProperties() {
        return new DataSourceProperties();
    }

    /**
     * get the reader data source.
     * @param readerDataSourceProperties reader Datasource properties.
     * @return reader DataSource
     */
    @Bean
    @ConditionalOnProperty(name = "spring.datasource.reader.url")
    @ConfigurationProperties(prefix = "spring.datasource.reader.hikari")
    public DataSource readerDataSource(final DataSourceProperties readerDataSourceProperties) {
        return readerDataSourceProperties.initializeDataSourceBuilder()
            .type(HikariDataSource.class)
            .build();
    }

    /**
     * reader Datasource properties.
     *
     * @return DataSourceProperties
     */
    @Bean
    @ConditionalOnProperty(name = "spring.datasource.reader.url")
    @ConfigurationProperties("spring.datasource.reader")
    public DataSourceProperties readerDataSourceProperties() {
        return new DataSourceProperties();
    }

    /**
     * Reader EntityManagerFactory.
     *
     * @param builder EntityManagerFactoryBuilder
     * @param readerDataSource DataSource
     * @return LocalContainerEntityManagerFactoryBean
     */
    @Bean(name = "readerEntityManagerFactory")
    @ConditionalOnProperty(name = "spring.datasource.reader.url")
    public LocalContainerEntityManagerFactoryBean readerEntityManagerFactory(
        final EntityManagerFactoryBuilder builder,
        @Qualifier("readerDataSource") final DataSource readerDataSource) {
        return builder
            .dataSource(readerDataSource)
            .packages("com.netflix.metacat.connector.polaris.store.entities") // Specify the package for your entities
            .persistenceUnit("reader")
            .build();
    }

    /**
     * Reader EntityManager.
     *
     * @param readerEntityManagerFactory EntityManagerFactory
     * @return EntityManager
     */
    @Bean(name = "readerEntityManager")
    @ConditionalOnProperty(name = "spring.datasource.reader.url")
    public EntityManager readerEntityManager(
        @Qualifier("readerEntityManagerFactory") final EntityManagerFactory readerEntityManagerFactory) {
        return readerEntityManagerFactory.createEntityManager();
    }

    /**
     * Reader Transaction Manager.
     *
     * @param readerEntityManagerFactory EntityManagerFactory
     * @return PlatformTransactionManager
     */
    @Bean(name = "readerTransactionManager")
    @ConditionalOnProperty(name = "spring.datasource.reader.url")
    public PlatformTransactionManager readerTransactionManager(
        @Qualifier("readerEntityManagerFactory") final EntityManagerFactory readerEntityManagerFactory) {
        return new JpaTransactionManager(readerEntityManagerFactory);
    }

    /**
     * Get an implementation of {@link PolarisStoreConnector}.
     *
     * @param repo    - PolarisDatabaseRepository
     * @param tblRepo - PolarisTableRepository
     * @return PolarisStoreConnector
     */
    @Bean
    public PolarisStoreService polarisStoreService(
        final PolarisDatabaseRepository repo, final PolarisTableRepository tblRepo) {
        return new PolarisStoreConnector(repo, tblRepo);
    }
}
