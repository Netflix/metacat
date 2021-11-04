//CHECKSTYLE:OFF
package com.netflix.metacat.metadata.store.data.repositories;

import com.netflix.metacat.metadata.store.configs.UserMetadataStoreConfig;
import com.netflix.metacat.metadata.store.data.entities.DataMetadataEntity;
import com.netflix.metacat.metadata.util.EntityTestUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.Assert;

import javax.transaction.Transactional;
import java.util.Optional;

/**
 * Test data metadata repository APIs
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {UserMetadataStoreConfig.class})
@ActiveProfiles(profiles = {"usermetadata-h2db"})
@Transactional
@AutoConfigureDataJpa
@Slf4j
public class DataMetadataRepositoryTests {

    @Autowired
    public DataMetadataRepository dataMetadataRepository;

    @Test
    public void testCreateAndGet() {
        DataMetadataEntity metadataEntity =
                dataMetadataRepository.save(EntityTestUtil.createDataMetadataEntity());

        // get the entity back
        DataMetadataEntity savedEntity = dataMetadataRepository.getOne(metadataEntity.getId());
        Assert.isTrue(savedEntity.equals(metadataEntity), "Retrieved entity should be the same");
        String testUri = savedEntity.getUri();
        log.info("Found test metadata entity Uri: {} and Id: {}",
                testUri, savedEntity.getId());

        // soft delete the entity
        savedEntity.setDeleted(true);
        dataMetadataRepository.saveAndFlush(savedEntity);
        Optional<DataMetadataEntity> entity =
                dataMetadataRepository.findByUri(testUri);
        Assert.isTrue(entity.isPresent() && entity.get().isDeleted(),
                "Entity should be soft-deleted");

        // delete the entity
        dataMetadataRepository.delete(savedEntity);
        Assert.isTrue(!dataMetadataRepository.findByUri(testUri).isPresent(),
                "Entity should be deleted");
    }
}
