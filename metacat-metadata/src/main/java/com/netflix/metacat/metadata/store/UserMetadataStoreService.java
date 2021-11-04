package com.netflix.metacat.metadata.store;

import com.netflix.metacat.metadata.store.data.repositories.DataMetadataRepository;
import com.netflix.metacat.metadata.store.data.repositories.DefinitionMetadataRepository;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Storage interface for user metadata entity operations.
 *
 * @author rveeramacheneni
 */
@Slf4j
public class UserMetadataStoreService {

    private final DefinitionMetadataRepository definitionMetadataRepository;
    private final DataMetadataRepository dataMetadataRepository;

    /**
     * Ctor.
     *
     * @param definitionMetadataRepository The definition metadata repository.
     * @param dataMetadataRepository       The data metadata repository.
     */
    @Autowired
    public UserMetadataStoreService(@NonNull final DefinitionMetadataRepository definitionMetadataRepository,
                                    @NonNull final DataMetadataRepository dataMetadataRepository) {
        this.definitionMetadataRepository = definitionMetadataRepository;
        this.dataMetadataRepository = dataMetadataRepository;
    }
}
