//CHECKSTYLE:OFF
package com.netflix.metacat.metadata.store.data.repositories;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.metadata.store.configs.UserMetadataStoreConfig;
import com.netflix.metacat.metadata.store.data.entities.DefinitionMetadataEntity;
import com.netflix.metacat.metadata.util.EntityTestUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import java.util.Optional;

/**
 * Test definition metadata repository APIs
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {UserMetadataStoreConfig.class})
@ActiveProfiles(profiles = {"usermetadata-h2db"})
@Transactional
@AutoConfigureDataJpa
public class DefinitionMetadataRepositoryTests {

    @Autowired
    private DefinitionMetadataRepository definitionMetadataRepository;

    @Test
    public void testCreateAndGet() {
        QualifiedName testQName = QualifiedName.fromString("prodhive/foo/bar");
        DefinitionMetadataEntity metadataEntity =
                definitionMetadataRepository.save(EntityTestUtil.createDefinitionMetadataEntity(testQName));

        // get the entity back
        DefinitionMetadataEntity savedEntity = definitionMetadataRepository.getOne(metadataEntity.getId());
        Assert.isTrue(savedEntity.equals(metadataEntity), "Retrieved entity should be the same");

        // soft delete the entity
        savedEntity.setDeleted(true);
        definitionMetadataRepository.saveAndFlush(savedEntity);
        Optional<DefinitionMetadataEntity> entity =
                definitionMetadataRepository.findByName(testQName);
        Assert.isTrue(entity.isPresent() && entity.get().isDeleted(),
                "Entity should be soft-deleted");

        // delete the entity
        definitionMetadataRepository.delete(savedEntity);
        Assert.isTrue(!definitionMetadataRepository.findByName(testQName).isPresent(),
                "Entity should be deleted");
    }
}
