//CHECKSTYLE:OFF
package com.netflix.metacat.metadata.store.data.repositories;

import com.netflix.metacat.metadata.store.configs.UserMetadataStoreConfig;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.annotation.Transactional;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {UserMetadataStoreConfig.class})
@ActiveProfiles(profiles = {"usermetadata-crdb"})
@Transactional
@AutoConfigureDataJpa
public class CrdbDataMetadataRepositoryTests extends DataMetadataRepositoryTests {
}
