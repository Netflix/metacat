package com.netflix.metacat.connector.polaris.configs;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import javax.sql.DataSource;

/**
 * Configuration for the reader data source.
 */
@Configuration
public class PolarisPersistenceReaderConfig {

    @Bean
    @ConditionalOnProperty(name = "spring.datasource.reader.url")
    @ConfigurationProperties(prefix = "spring.datasource.reader")
    public DataSourceProperties readerDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean
    @ConditionalOnProperty(name = "spring.datasource.reader.url")
    @ConfigurationProperties(prefix = "spring.datasource.reader.hikari")
    public DataSource readerDataSource() {
        return readerDataSourceProperties().initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    @Bean(name = "readerEntityManagerFactory")
    @ConditionalOnProperty(name = "spring.datasource.reader.url")
    public LocalContainerEntityManagerFactoryBean readerEntityManagerFactory(
        @Qualifier("readerDataSource") DataSource readerDataSource) {
        LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
        em.setDataSource(readerDataSource);
        em.setPackagesToScan("com.netflix.metacat.connector.polaris.store.entities");
        em.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
        return em;
    }

    @Bean(name = "readEntityManager")
    @ConditionalOnProperty(name = "spring.datasource.reader.url")
    public EntityManager readEntityManager(
        @Qualifier("readerEntityManagerFactory") EntityManagerFactory readerEntityManagerFactory) {
        return readerEntityManagerFactory.createEntityManager();
    }

    @Bean(name = "readerTransactionManager")
    @ConditionalOnProperty(name = "spring.datasource.reader.url")
    public PlatformTransactionManager readerTransactionManager(
        @Qualifier("readerEntityManagerFactory") EntityManagerFactory readerEntityManagerFactory) {
        return new JpaTransactionManager(readerEntityManagerFactory);
    }
}
