package com.netflix.metacat.connector.polaris;


import com.netflix.metacat.connector.polaris.configs.PolarisPersistenceConfig;
import com.netflix.metacat.connector.polaris.store.PolarisStoreConnectorTest;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;

/**
 * Test persistence operations on Database objects.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {PolarisPersistenceConfig.class})
@ActiveProfiles(profiles = {"polaris_functional_test"})
@AutoConfigureDataJpa
public class PolarisStoreConnectorFunctionalTest extends PolarisStoreConnectorTest {

    /**
     * Test getTableEntities.
     */
    @Test
    public void testGetTableEntities() {
        // Create the db
        final String dbName = generateDatabaseName();
        createDB(dbName);

        try {
            // pause execution for 10000 milliseconds (10 seconds)
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            System.out.println("Sleep was interrupted");
        }

        // Test when db is empty
        List<PolarisTableEntity> entities = getPolarisConnector().getTableEntities(dbName, "", 1);
        Assert.assertEquals(0, entities.size());


        // Add some tables
        final String tblNameA = "A_" + generateTableName();
        final String tblNameB = "B_" + generateTableName();
        final String tblNameC = "C_" + generateTableName();
        createTable(dbName, tblNameA);
        createTable(dbName, tblNameB);
        createTable(dbName, tblNameC);

        try {
            // pause execution for 10000 milliseconds (10 seconds)
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            System.out.println("Sleep was interrupted");
        }

        // Test pagination and sort
        entities = getPolarisConnector().getTableEntities(dbName, "", 1);
        Assert.assertEquals(3, entities.size());
        Assert.assertEquals(tblNameA, entities.get(0).getTblName());
        Assert.assertEquals(tblNameB, entities.get(1).getTblName());
        Assert.assertEquals(tblNameC, entities.get(2).getTblName());

        entities = getPolarisConnector().getTableEntities(dbName, "", 2);
        Assert.assertEquals(3, entities.size());
        Assert.assertEquals(tblNameA, entities.get(0).getTblName());
        Assert.assertEquals(tblNameB, entities.get(1).getTblName());
        Assert.assertEquals(tblNameC, entities.get(2).getTblName());

        entities = getPolarisConnector().getTableEntities(dbName, "", 3);
        Assert.assertEquals(3, entities.size());
        Assert.assertEquals(tblNameA, entities.get(0).getTblName());
        Assert.assertEquals(tblNameB, entities.get(1).getTblName());
        Assert.assertEquals(tblNameC, entities.get(2).getTblName());

        entities = getPolarisConnector().getTableEntities(dbName, "", 4);
        Assert.assertEquals(3, entities.size());
        Assert.assertEquals(tblNameA, entities.get(0).getTblName());
        Assert.assertEquals(tblNameB, entities.get(1).getTblName());
        Assert.assertEquals(tblNameC, entities.get(2).getTblName());
    }
}
