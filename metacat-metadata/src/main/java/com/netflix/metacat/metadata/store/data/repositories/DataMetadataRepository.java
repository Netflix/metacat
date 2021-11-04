package com.netflix.metacat.metadata.store.data.repositories;

import com.netflix.metacat.metadata.store.data.entities.DataMetadataEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * The data metadata entity repository.
 *
 * @author rveeramacheneni
 */
@Repository
public interface DataMetadataRepository extends JpaRepository<DataMetadataEntity, String> {
    /**
     * Find a data metadata entity using the given uri.
     *
     * @param uri The uri of the entity.
     * @return The data metadata entity.
     */
    Optional<DataMetadataEntity> findByUri(String uri);
}
