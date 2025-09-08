package com.netflix.metacat.connector.polaris.configs;

import com.netflix.metacat.connector.polaris.store.routing.RoutingDataSource;
import com.netflix.metacat.connector.polaris.store.PolarisStoreConnector;
import com.netflix.metacat.connector.polaris.store.PolarisStoreService;
import com.netflix.metacat.connector.polaris.store.repos.PolarisDatabaseRepository;
import com.netflix.metacat.connector.polaris.store.repos.PolarisTableRepository;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * The Polaris Store Persistence config.
 *
 * This configuration sets up the primary and replica datasources,
 * a routing datasource to switch between them, and the JPA repositories for Polaris.
 */
@Configuration
@EntityScan("com.netflix.metacat.connector.polaris.store.entities")
@EnableJpaRepositories("com.netflix.metacat.connector.polaris.store.repos")
@EnableJpaAuditing
@EnableTransactionManagement(proxyTargetClass = true)
public class PolarisPersistenceConfig {

    /**
     * Loads the configuration properties for the primary (write) datasource.
     * Properties are expected under 'spring.datasource'.
     *
     * @return DataSourceProperties for the primary datasource
     */
    @Bean
    @ConfigurationProperties("spring.datasource")
    public DataSourceProperties primaryDataSourceProperties() {
        return new DataSourceProperties();
    }

    /**
     * Creates the primary (write) datasource bean using HikariCP.
     * Uses properties loaded by primaryDataSourceProperties.
     *
     * @param properties DataSourceProperties for the primary datasource
     * @return DataSource for primary (write) operations
     */
    @Bean(name = "primaryDataSource")
    @ConfigurationProperties("spring.datasource.hikari")
    public DataSource primaryDataSource(@Qualifier("primaryDataSourceProperties")
                                            final DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    /**
     * Loads the configuration properties for the replica (read) datasource.
     * Properties are expected under 'spring.datasource.replica'.
     *
     * @return DataSourceProperties for the replica datasource
     */
    @Bean
    @ConfigurationProperties("spring.datasource.reader")
    public DataSourceProperties replicaDataSourceProperties() {
        return new DataSourceProperties();
    }

    /**
     * Creates the replica (read) datasource bean using HikariCP.
     * Uses properties loaded by replicaDataSourceProperties.
     *
     * @param properties DataSourceProperties for the replica datasource
     * @return DataSource for replica (read) operations
     */
    @Bean(name = "replicaDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.reader.hikari")
    public DataSource replicaDataSource(@Qualifier("replicaDataSourceProperties")
                                            final DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    /**
     * Routing datasource bean that switches between primary and replica datasources
     * based on the current context (e.g., read vs. write operations).
     * This is marked as @Primary so Spring JPA and transaction managers use it by default.
     *
     * @param primaryDataSource the primary (write) datasource
     * @param replicaDataSource the replica (read) datasource
     * @return RoutingDataSource that delegates to primary or replica
     */
    @Bean
    @Primary
    public DataSource routingDataSource(
        @Qualifier("primaryDataSource") final DataSource primaryDataSource,
        @Qualifier("replicaDataSource") final DataSource replicaDataSource) {
        final Map<Object, Object> dataSources = new HashMap<>();
        dataSources.put("PRIMARY", primaryDataSource);
        dataSources.put("REPLICA", replicaDataSource);

        final RoutingDataSource routingDataSource = new RoutingDataSource();
        routingDataSource.setTargetDataSources(dataSources);
        routingDataSource.setDefaultTargetDataSource(primaryDataSource);
        return routingDataSource;
    }


    @Bean
    public LocalContainerEntityManagerFactoryBean entityManagerFactory(
        @Qualifier("routingDataSource") DataSource dataSource) {
        LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
        em.setDataSource(dataSource);
        em.setPackagesToScan("com.netflix.metacat.connector.polaris.store.entities");

        em.setJpaVendorAdapter(new HibernateJpaVendorAdapter());

//        Properties properties = new Properties();
//        properties.put("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect"); // or your DB dialect
//        em.setJpaProperties(properties);

        return em;
    }

    @Bean
    public PlatformTransactionManager transactionManager(
        @Qualifier("entityManagerFactory") EntityManagerFactory emf) {
        return new JpaTransactionManager(emf);
    }


    /**
     * Provides the PolarisStoreService bean, which implements the core store logic.
     * This service is constructed using the database and table repositories.
     *
     * @param repo PolarisDatabaseRepository for database entities
     * @param tblRepo PolarisTableRepository for table entities
     * @return PolarisStoreService implementation
     */
    @Bean
    public PolarisStoreService polarisStoreService(
        final PolarisDatabaseRepository repo, final PolarisTableRepository tblRepo) {
        return new PolarisStoreConnector(repo, tblRepo);
    }
}
