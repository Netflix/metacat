package com.netflix.metacat.connector.polaris;


import com.netflix.metacat.connector.polaris.configs.PolarisPersistenceConfig;
import com.netflix.metacat.connector.polaris.store.PolarisStoreConnectorTest;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Test persistence operations on Database objects.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {PolarisPersistenceConfig.class})
@ActiveProfiles(profiles = {"polaris_functional_test"})
@AutoConfigureDataJpa
public class PolarisStoreConnectorFunctionalTest extends PolarisStoreConnectorTest {
}
