package com.netflix.metacat.connector.polaris;


import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * The Polaris Store Persistence config.
 *
 */
@Configuration
@EntityScan("com.netflix.metacat.connector.polaris")
@EnableJpaRepositories("com.netflix.metacat.connector.polaris")
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
   * Get an implementation of {@link PolarisConnector}.
   * @param repo - PolarisDatabaseRepository
   * @param tblRepo - PolarisTableRepository
   * @return PolarisConnector
   */
  @Bean
  public PolarisConnector polarisConnector(
      final PolarisDatabaseRepository repo, final PolarisTableRepository tblRepo) {
    return new PolarisConnector(repo, tblRepo);
  }
}
