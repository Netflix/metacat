package com.netflix.metacat.connector.polaris;


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Test persistence operations on Database objects.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {PolarisPersistenceConfig.class})
@ActiveProfiles(profiles = {"polaristest"})
@AutoConfigureDataJpa
public class PolarisConnectorTest {
    private static final String DB_NAME_FOO = "foo";

    @Autowired
    private PolarisDatabaseRepository repo;

    @Autowired
    private PolarisConnector polarisConnector;

    /**
     * Test set up.
     */
    @Before
    @After
    public void setup() {
        Assert.assertNotNull(repo);
        repo.deleteAll();
    }

    /**
     * Test Database object creation and persistence.
     */
    @Test
    public void testCreateDB() {
        final PolarisDatabaseEntity newEntity = new PolarisDatabaseEntity(DB_NAME_FOO);
        final PolarisDatabaseEntity savedEntity = repo.save(newEntity);
        Assert.assertEquals(DB_NAME_FOO, newEntity.getDbName());
        Assert.assertEquals(0L, savedEntity.getVersion().longValue());
    }
}
