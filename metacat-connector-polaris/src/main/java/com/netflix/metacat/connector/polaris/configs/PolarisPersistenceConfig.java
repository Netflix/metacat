package com.netflix.metacat.connector.polaris.configs;


import com.netflix.metacat.connector.polaris.store.PolarisStoreConnector;
import com.netflix.metacat.connector.polaris.store.PolarisStoreService;
import com.netflix.metacat.connector.polaris.store.repos.PolarisDatabaseRepository;
import com.netflix.metacat.connector.polaris.store.repos.PolarisTableRepository;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import javax.sql.DataSource;

/**
 * The Polaris Store Persistence config.
 *
 */
@Configuration
@EntityScan("com.netflix.metacat.connector.polaris.store.entities")
@EnableJpaRepositories("com.netflix.metacat.connector.polaris.store.repos")
@EnableAutoConfiguration
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
   * @return DataSourceProperties
   */
  @Bean
  @Primary
  @ConfigurationProperties("spring.datasource")
  public DataSourceProperties dataSourceProperties() {
    return new DataSourceProperties();
  }

  /**
   * Get an implementation of {@link PolarisStoreConnector}.
   * @param repo - PolarisDatabaseRepository
   * @param tblRepo - PolarisTableRepository
   * @return PolarisStoreConnector
   */
  @Bean
  public PolarisStoreService polarisStoreService(
      final PolarisDatabaseRepository repo, final PolarisTableRepository tblRepo) {
    return new PolarisStoreConnector(repo, tblRepo);
  }
}
