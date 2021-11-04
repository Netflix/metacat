package com.netflix.metacat.metadata.store.configs;

import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.json.MetacatJsonLocator;
import com.netflix.metacat.metadata.store.UserMetadataStoreService;
import com.netflix.metacat.metadata.store.data.repositories.DataMetadataRepository;
import com.netflix.metacat.metadata.store.data.repositories.DefinitionMetadataRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * The user metadata store config.
 *
 * @author rveeramacheneni
 */
@Configuration
@EntityScan("com.netflix.metacat.metadata.store.data.*")
@EnableJpaRepositories("com.netflix.metacat.metadata.store.data.*")
public class UserMetadataStoreConfig {

    /**
     * The user metadata store service.
     *
     * @param definitionMetadataRepository The definition metadata repository.
     * @param dataMetadataRepository The data metadata repository.
     * @return the constructed bean.
     */
    @Bean
    public UserMetadataStoreService userMetadataStoreService(
            final DefinitionMetadataRepository definitionMetadataRepository,
            final DataMetadataRepository dataMetadataRepository) {
        return new UserMetadataStoreService(definitionMetadataRepository, dataMetadataRepository);
    }

    /**
     * Store metacat JSON Handler.
     *
     * @return The JSON handler
     */
    @Bean
    @ConditionalOnMissingBean(MetacatJson.class)
    public MetacatJson metacatJson() {
        return new MetacatJsonLocator();
    }
}
