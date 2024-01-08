package com.netflix.metacat.connector.polaris.store;


import com.netflix.metacat.connector.polaris.common.PolarisUtils;
import com.netflix.metacat.connector.polaris.configs.PolarisPersistenceConfig;
import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import com.netflix.metacat.connector.polaris.store.repos.PolarisDatabaseRepository;
import com.netflix.metacat.connector.polaris.store.repos.PolarisTableRepository;
import lombok.Getter;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Optional;
import java.util.Random;

/**
 * Test persistence operations on Database objects.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {PolarisPersistenceConfig.class})
@ActiveProfiles(profiles = {"polaristest"})
@AutoConfigureDataJpa
@Getter
public class PolarisStoreConnectorTest {
    private static final String DB_NAME_FOO = "foo";
    private static final String TBL_NAME_BAR = "bar";
    private static final String DEFAULT_METACAT_USER = "metacat_user";
    private static Random random = new Random(System.currentTimeMillis());

    @Autowired
    private PolarisDatabaseRepository repo;

    @Autowired
    private PolarisTableRepository tblRepo;

    @Autowired
    private PolarisStoreConnector polarisConnector;

    public static String generateDatabaseName() {
        return DB_NAME_FOO + "_" + random.nextLong();
    }

    public static String generateTableName() {
        return TBL_NAME_BAR + "_" + random.nextLong();
    }

    public PolarisDatabaseEntity createDB(final String dbName) {
        final String location = "file://temp";
        final PolarisDatabaseEntity entity = polarisConnector.createDatabase(dbName, location, "metacat_user");

        // assert that database exists, post-creation.
        Assert.assertTrue(polarisConnector.databaseExistsById(entity.getDbId()));
        Assert.assertTrue(polarisConnector.databaseExists(dbName));

        Assert.assertEquals(0L, entity.getVersion().longValue());
        Assert.assertTrue(entity.getDbId().length() > 0);
        Assert.assertEquals(dbName, entity.getDbName());
        Assert.assertEquals(location, entity.getLocation());
        Assert.assertEquals(DEFAULT_METACAT_USER, entity.getAudit().getCreatedBy());

        final Optional<PolarisDatabaseEntity> fetchedEntity = polarisConnector.getDatabase(dbName);
        Assert.assertTrue(fetchedEntity.isPresent());
        Assert.assertEquals(entity, fetchedEntity.get());
        return entity;
    }

    public PolarisTableEntity createTable(final String dbName, final String tblName) {
        final PolarisTableEntity entity = polarisConnector.createTable(dbName, tblName,
                "loc", PolarisUtils.DEFAULT_METACAT_USER);

        Assert.assertTrue(polarisConnector.tableExistsById(entity.getTblId()));
        Assert.assertTrue(polarisConnector.tableExists(dbName, tblName));

        Assert.assertTrue(entity.getTblId().length() > 0);
        Assert.assertTrue(entity.getVersion() >= 0);

        Assert.assertEquals(dbName, entity.getDbName());
        Assert.assertEquals(tblName, entity.getTblName());

        final Optional<PolarisTableEntity> fetchedEntity = polarisConnector.getTable(dbName, tblName);
        Assert.assertTrue(fetchedEntity.isPresent());
        Assert.assertEquals(entity, fetchedEntity.get());

        return entity;
    }

    /**
     * Test Database object creation and persistence.
     */
    @Test
    public void testCreateDB() {
        final PolarisDatabaseEntity savedEntity = createDB(generateDatabaseName());
    }

    /**
     * Test that a table cannot be created if database is absent.
     */
    @Test
    public void testTableCreationFailIfDatabaseIsAbsent() {
        Assertions.assertThrows(DataAccessException.class, () ->
            polarisConnector.createTable(generateDatabaseName(), generateTableName(),
                    "loc", PolarisUtils.DEFAULT_METACAT_USER));
    }

    /**
     * Test table creation if database exists.
     * Verify table deletion
     */
    @Test
    public void testTableCreationAndDeletion() {
        final String dbName = generateDatabaseName();
        final String tblName = generateTableName();
        final PolarisDatabaseEntity dbEntity = createDB(dbName);
        final PolarisTableEntity tblEntity = createTable(dbName, tblName);

        polarisConnector.deleteTable(dbName, tblName);
        Assert.assertFalse(polarisConnector.tableExistsById(tblEntity.getTblId()));
    }

    /**
     * Test to verify that table name can be updated.
     */
    @Test
    public void testTableUpdate() {
        // Create Table Entity in DB
        final String dbName = generateDatabaseName();
        final String tblName = generateTableName();
        final PolarisDatabaseEntity dbEntity = createDB(dbName);
        final PolarisTableEntity tblEntity = createTable(dbName, tblName);

        // Update table name
        final String newTblName = generateTableName();
        tblEntity.setTblName(newTblName);
        final PolarisTableEntity updatedTblEntity = polarisConnector.saveTable(tblEntity);
        Assert.assertEquals(newTblName, updatedTblEntity.getTblName());
    }

    /**
     * Test to validate that the table can be created via a PolarisTableEntity parameter.
     * Also tests that metadata_location is getting stored.
     */
    @Test
    public void createTableWithSaveApi() {
        final String dbName = generateDatabaseName();
        createDB(dbName);

        final String tblName = generateTableName();
        final String metadataLocation = "s3/s3n://dataoven-prod/hive/dataoven_prod/warehouse/foo";
        final PolarisTableEntity e = new PolarisTableEntity(dbName, tblName, "metacatuser");
        e.setMetadataLocation(metadataLocation);

        final PolarisTableEntity savedEntity = polarisConnector.saveTable(e);
        Assert.assertEquals(metadataLocation, savedEntity.getMetadataLocation());
    }

    /**
     * Test to verify that compare-and-swap update of the metadata location works as expected.
     */
    @Test
    public void updateMetadataLocation() {
        final String dbName = generateDatabaseName();
        createDB(dbName);

        final String tblName = generateTableName();
        final String metadataLocation = "s3/s3n://dataoven-prod/hive/dataoven_prod/warehouse/foo";
        final PolarisTableEntity e = new PolarisTableEntity(dbName, tblName, "metacatuser");
        e.setMetadataLocation(metadataLocation);
        final PolarisTableEntity savedEntity = polarisConnector.saveTable(e);

        final String newLocation = "s3/s3n://dataoven-prod/hive/dataoven_prod/warehouse/bar";

        // update should fail since the expected location is not going to match.
        boolean updatedSuccess = polarisConnector.updateTableMetadataLocation(
                dbName, tblName, "unexpected_location",
                newLocation, PolarisUtils.DEFAULT_METACAT_USER);
        Assert.assertFalse(updatedSuccess);

        // successful update should happen.
        updatedSuccess = polarisConnector.updateTableMetadataLocation(dbName, tblName, metadataLocation,
                newLocation, "new_user");
        Assert.assertTrue(updatedSuccess);
        final PolarisTableEntity updatedEntity = polarisConnector.
                getTable(dbName, tblName).orElseThrow(() -> new RuntimeException("Expected to find saved entity"));
        Assert.assertEquals(updatedEntity.getPreviousMetadataLocation(), metadataLocation);

        // after the successful update, the same call should fail, since the current metadataLocation has changed.
        updatedSuccess = polarisConnector.updateTableMetadataLocation(dbName, tblName, metadataLocation,
                newLocation, PolarisUtils.DEFAULT_METACAT_USER);
        Assert.assertFalse(updatedSuccess);
    }

    /**
     * Test updateLocation(...) while save(...) is called in interleaved fashion.
     */
    @Test
    public void updateMetadataLocationWithInterleavedSave() {
        final String dbName = generateDatabaseName();
        createDB(dbName);

        final String tblName = generateTableName();
        final String location0 = "s3/s3n://dataoven-prod/hive/dataoven_prod/warehouse/location0";
        final PolarisTableEntity e = new PolarisTableEntity(dbName, tblName, "metacatuser");
        e.setMetadataLocation(location0);
        final PolarisTableEntity savedEntity = polarisConnector.saveTable(e);

        final String location1 = "s3/s3n://dataoven-prod/hive/dataoven_prod/warehouse/location1";

        // update the metadata location.
        final boolean updatedSuccess =
            polarisConnector.updateTableMetadataLocation(dbName, tblName, location0, location1, "new_user");
        Assert.assertTrue(updatedSuccess);


        final String location2 = "s3/s3n://dataoven-prod/hive/dataoven_prod/warehouse/location2";
        // At this point, savedEntity is stale, and any updates to savedEntity should not be allowed
        // to persist.
        savedEntity.setMetadataLocation(location2);
        Assertions.assertThrows(OptimisticLockingFailureException.class, () -> {
            polarisConnector.saveTable(savedEntity);
        });
    }
}
