
package com.netflix.metacat.connector.polaris;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import com.netflix.metacat.common.server.connectors.exception.TablePreconditionFailedException;
import com.netflix.metacat.common.server.connectors.model.FieldInfo;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.server.properties.DefaultConfigImpl;
import com.netflix.metacat.common.server.properties.MetacatProperties;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.connector.hive.commonview.CommonViewHandler;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableCriteriaImpl;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableHandler;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableOpWrapper;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableOpsProxy;
import com.netflix.metacat.connector.polaris.configs.PolarisPersistenceConfig;
import com.netflix.metacat.connector.polaris.mappers.PolarisTableMapper;
import com.netflix.metacat.connector.polaris.store.PolarisStoreService;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import com.netflix.spectator.api.NoopRegistry;
import lombok.Getter;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import spock.lang.Shared;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Test PolarisConnectorTableService.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {PolarisPersistenceConfig.class})
@ActiveProfiles(profiles = {"polarisconnectortest"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@AutoConfigureDataJpa
@Getter
public class PolarisConnectorTableServiceTest {
    public static final String CATALOG_NAME = "catalog_name";
    public static final String DB_NAME = "db_name";
    public static final QualifiedName DB_QUALIFIED_NAME = QualifiedName.ofDatabase(CATALOG_NAME, DB_NAME);

    @Autowired
    private PolarisStoreService polarisStoreService;

    @Shared
    private ConnectorRequestContext requestContext = new ConnectorRequestContext();

    @Shared
    private ThreadServiceManager serviceManager = Mockito.mock(ThreadServiceManager.class);

    @Shared
    private ConnectorContext connectorContext;

    @Shared
    private PolarisConnectorDatabaseService polarisDBService;

    @Shared
    private PolarisConnectorTableService polarisTableService;

    @Shared
    private Environment env = Mockito.mock(Environment.class);

    /**
     * Initialization.
     */
    @BeforeEach
    public void init() {
        final String location = "file://temp";
        polarisStoreService.createDatabase(DB_NAME, location, "metacat_user");
        connectorContext = new ConnectorContext(CATALOG_NAME, CATALOG_NAME, "polaris",
            new DefaultConfigImpl(new MetacatProperties(null)), new NoopRegistry(), null,  Maps.newHashMap());
        polarisDBService = new PolarisConnectorDatabaseService(polarisStoreService, connectorContext);
        polarisTableService = new PolarisConnectorTableService(
            polarisStoreService,
            CATALOG_NAME,
            polarisDBService,
            new HiveConnectorInfoConverter(new HiveTypeConverter()),
            new IcebergTableHandler(connectorContext,
                new IcebergTableCriteriaImpl(connectorContext),
                new IcebergTableOpWrapper(connectorContext, serviceManager),
                new IcebergTableOpsProxy()),
            new CommonViewHandler(connectorContext),
            new PolarisTableMapper(CATALOG_NAME),
            connectorContext);
    }

    /**
     * Test table exists.
     */
    @Test
    public void testTableExists() {
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table1");
        final TableInfo tableInfo = TableInfo.builder()
            .name(qualifiedName)
            .metadata(ImmutableMap.of("table_type", "ICEBERG", "metadata_location", "loc1"))
            .build();
        boolean exists = polarisTableService.exists(requestContext, qualifiedName);
        Assert.assertFalse(exists);
        polarisTableService.create(requestContext, tableInfo);
        exists = polarisTableService.exists(requestContext, qualifiedName);
        Assert.assertTrue(exists);
    }

    /**
     * Test table rename.
     */
    @Test
    public void testTableRename() {
        final QualifiedName nameOld = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table1");
        final QualifiedName nameNew = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table2");
        final TableInfo tableInfo = TableInfo.builder()
            .name(nameOld)
            .metadata(ImmutableMap.of("table_type", "ICEBERG", "metadata_location", "loc1"))
            .build();
        polarisTableService.create(requestContext, tableInfo);
        boolean existsOld = polarisTableService.exists(requestContext, nameOld);
        Assert.assertTrue(existsOld);
        boolean existsNew = polarisTableService.exists(requestContext, nameNew);
        Assert.assertFalse(existsNew);
        polarisTableService.rename(requestContext, nameOld, nameNew);
        existsOld = polarisTableService.exists(requestContext, nameOld);
        Assert.assertFalse(existsOld);
        existsNew = polarisTableService.exists(requestContext, nameNew);
        Assert.assertTrue(existsNew);
    }

    /**
     * Test delete table.
     */
    @Test
    public void testDeleteTable() {
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table");
        final TableInfo tableInfo = TableInfo.builder()
            .name(qualifiedName)
            .metadata(ImmutableMap.of("table_type", "ICEBERG", "metadata_location", "loc1"))
            .build();
        polarisTableService.create(requestContext, tableInfo);
        boolean exists = polarisTableService.exists(requestContext, qualifiedName);
        Assert.assertTrue(exists);
        polarisTableService.delete(requestContext, qualifiedName);
        exists = polarisTableService.exists(requestContext, qualifiedName);
        Assert.assertFalse(exists);
    }

    /**
     * Test get table using metadata json resource file.
     */
    @Test
    public void testGetTable() {
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table1");
        final String location = "src/test/resources/metadata/00001-abf48887-aa4f-4bcc-9219-1e1721314ee1.metadata.json";
        final TableInfo tableInfo = TableInfo.builder()
            .name(qualifiedName)
            .metadata(ImmutableMap.of("table_type", "ICEBERG", "metadata_location", location))
            .build();
        polarisTableService.create(requestContext, tableInfo);
        final TableInfo tableResult = polarisTableService.get(requestContext, qualifiedName);
        // check schema info correctly parsed from iceberg metadata file
        final List<FieldInfo> fields = tableResult.getFields();
        Assert.assertEquals(fields.size(), 3);
        Assert.assertEquals(fields.get(0).getName(), "id");
        Assert.assertEquals(fields.get(0).getComment(), "1st field");
        Assert.assertEquals(fields.get(0).getSourceType(), "long");
        Assert.assertEquals(fields.get(1).getName(), "data");
        Assert.assertEquals(fields.get(1).getComment(), "2nd field");
        Assert.assertEquals(fields.get(1).getSourceType(), "string");
        Assert.assertEquals(fields.get(2).getName(), "dateint");
        Assert.assertEquals(fields.get(2).getComment(), "3rd field");
        Assert.assertEquals(fields.get(2).getSourceType(), "int");
    }

    /**
     * Test table serde fields.
     */
    @Test
    public void testTableSerde() {
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table1");
        final String location = "src/test/resources/metadata/00000-9b5d4c36-130c-4288-9599-7d850c203d11.metadata.json";
        final TableInfo tableInfo = TableInfo.builder()
            .name(qualifiedName)
            .metadata(ImmutableMap.of("table_type", "ICEBERG", "metadata_location", location))
            .build();
        polarisTableService.create(requestContext, tableInfo);
        final TableInfo tableResult = polarisTableService.get(requestContext, qualifiedName);
        // check serde info
        Assert.assertNotNull(tableResult.getSerde());
        Assert.assertEquals(tableResult.getSerde().getUri(), "src/test/resources");
        Assert.assertEquals(tableResult.getSerde().getInputFormat(), "org.apache.hadoop.mapred.FileInputFormat");
        Assert.assertEquals(tableResult.getSerde().getOutputFormat(), "org.apache.hadoop.mapred.FileOutputFormat");
        Assert.assertEquals(tableResult.getSerde().getSerializationLib(),
            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    }

    /**
     * Test table params not returned during table get
     */
    @Test
    public void testTableParams() {
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table1");
        final String location = "src/test/resources/metadata/00000-9b5d4c36-130c-4288-9599-7d850c203d11.metadata.json";
        final Map<String, String> metadata = ImmutableMap.of("table_type", "ICEBERG", "metadata_location",
            location, "test_metadata_key", "test_metadata_value");
        final TableInfo tableInfo = TableInfo.builder()
            .name(qualifiedName)
            .metadata(metadata)
            .build();
        polarisTableService.create(requestContext, tableInfo);
        final TableInfo tableResult = polarisTableService.get(requestContext, qualifiedName);
        // We don't want the test_metadata_key value to appear in GET requests.
        Assert.assertEquals(tableResult.getMetadata().get("metadata_location"), location);
        Assert.assertFalse(tableResult.getMetadata().containsKey("test_metadata_key"));

        // Even though it doesn't show in the response, we want the params to be there in the DB.
        PolarisTableEntity entity = polarisStoreService.getTable(DB_NAME, "table1")
            .orElseThrow(() -> new RuntimeException("Expected table entity to be present"));
        Assert.assertTrue(entity.getParams().get("test_metadata_key").equals("test_metadata_value"));
    }

    /**
     * Test update table reject cases.
     */
    @Test
    public void testUpdateTableReject() {
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table1");
        final String location0 = "src/test/resources/metadata/00000-9b5d4c36-130c-4288-9599-7d850c203d11.metadata.json";
        final String location1 = "src/test/resources/metadata/00001-abf48887-aa4f-4bcc-9219-1e1721314ee1.metadata.json";
        final String location2 = "src/test/resources/metadata/00002-2d6c1951-31d5-4bea-8edd-e35746b172f3.metadata.json";
        final Map<String, String> metadata = new HashMap<>();
        metadata.put("metadata_location", location0);
        final TableInfo tableInfo0 = TableInfo.builder().name(qualifiedName).metadata(metadata).build();
        polarisTableService.create(requestContext, tableInfo0);
        final TableInfo tableResult0 = polarisTableService.get(requestContext, qualifiedName);
        Assert.assertEquals(tableResult0.getMetadata().get("metadata_location"), location0);
        // check update location without setting location fails
        metadata.put("previous_metadata_location", location1);
        metadata.remove("metadata_location");
        final TableInfo tableInfo1 = TableInfo.builder().name(qualifiedName).metadata(metadata).build();
        Assertions.assertThrows(InvalidMetaException.class,
            () -> polarisTableService.update(requestContext, tableInfo1));
        // check update location to new location equals blank fails
        metadata.put("previous_metadata_location", location0);
        metadata.put("metadata_location", "");
        final TableInfo tableInfo2 = TableInfo.builder().name(qualifiedName).metadata(metadata).build();
        Assertions.assertThrows(InvalidMetaException.class,
            () -> polarisTableService.update(requestContext, tableInfo2));
        // check update location existing and previous location do not match fails
        metadata.put("previous_metadata_location", location1);
        metadata.put("metadata_location", location2);
        final TableInfo tableInfo3 = TableInfo.builder().name(qualifiedName).metadata(metadata).build();
        Assertions.assertThrows(TablePreconditionFailedException.class,
            () -> polarisTableService.update(requestContext, tableInfo3));
    }

    /**
     * Test update table using metadata json resource file.
     */
    @Test
    public void testUpdateTableAccept() {
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table1");
        final String location0 = "src/test/resources/metadata/00000-9b5d4c36-130c-4288-9599-7d850c203d11.metadata.json";
        final TableInfo tableInfo0 = TableInfo.builder()
            .name(qualifiedName)
            .metadata(ImmutableMap.of("metadata_location", location0))
            .build();
        polarisTableService.create(requestContext, tableInfo0);
        final TableInfo tableResult0 = polarisTableService.get(requestContext, qualifiedName);
        Assert.assertEquals(tableResult0.getMetadata().get("metadata_location"), location0);
        final String location1 = "src/test/resources/metadata/00001-abf48887-aa4f-4bcc-9219-1e1721314ee1.metadata.json";
        final TableInfo tableInfo1 = TableInfo.builder()
            .name(qualifiedName)
            .metadata(ImmutableMap.of("previous_metadata_location", location0, "metadata_location", location1))
            .build();
        polarisTableService.update(requestContext, tableInfo1);
        final TableInfo tableResult1 = polarisTableService.get(requestContext, qualifiedName);
        Assert.assertEquals(tableResult1.getMetadata().get("metadata_location"), location1);
    }

    /**
     * Test update table using metadata json resource file and new param.
     */
    @Test
    public void testUpdateTableAcceptWithParams() {
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table1");
        final String location0 = "src/test/resources/metadata/00000-9b5d4c36-130c-4288-9599-7d850c203d11.metadata.json";
        final TableInfo tableInfo0 = TableInfo.builder()
            .name(qualifiedName)
            .metadata(ImmutableMap.of("metadata_location", location0, "test_param_key", "test_param_value"))
            .build();
        polarisTableService.create(requestContext, tableInfo0);
        final TableInfo tableResult0 = polarisTableService.get(requestContext, qualifiedName);
        Assert.assertEquals(tableResult0.getMetadata().get("metadata_location"), location0);
        Assert.assertFalse(tableResult0.getMetadata().containsKey("test_param_key"));

        PolarisTableEntity entity = polarisStoreService.getTable(DB_NAME, "table1")
            .orElseThrow(() -> new RuntimeException("Expected table entity to be present"));
        Assert.assertTrue(entity.getParams().get("test_param_key").equals("test_param_value"));

        final String location1 = "src/test/resources/metadata/00001-abf48887-aa4f-4bcc-9219-1e1721314ee1.metadata.json";
        final TableInfo tableInfo1 = TableInfo.builder()
            .name(qualifiedName)
            .metadata(ImmutableMap.of("previous_metadata_location", location0, "metadata_location", location1,
                "test_param_key", "test_param_value2"))
            .build();
        polarisTableService.update(requestContext, tableInfo1);
        final TableInfo tableResult1 = polarisTableService.get(requestContext, qualifiedName);
        Assert.assertEquals(tableResult1.getMetadata().get("metadata_location"), location1);
        Assert.assertEquals(tableResult1.getMetadata().get("table_type"), "ICEBERG");
        Assert.assertFalse(tableResult1.getMetadata().containsKey("test_param_key"));

        PolarisTableEntity updatedEntity = polarisStoreService.getTable(DB_NAME, "table1")
            .orElseThrow(() -> new RuntimeException("Expected table entity to be present"));
        Assert.assertTrue(updatedEntity.getParams().get("test_param_key").equals("test_param_value2"));
    }

    /**
     * Test update passes but metadata is unchanged when only params are provided, no location.
     */
    @Test
    public void testUpdateTableSkipOnlyParams() {
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table1");
        final String location0 = "src/test/resources/metadata/00000-9b5d4c36-130c-4288-9599-7d850c203d11.metadata.json";
        final TableInfo tableInfo0 = TableInfo.builder()
            .name(qualifiedName)
            .metadata(ImmutableMap.of("metadata_location", location0))
            .build();
        polarisTableService.create(requestContext, tableInfo0);
        final TableInfo tableResult0 = polarisTableService.get(requestContext, qualifiedName);
        Assert.assertEquals(tableResult0.getMetadata().get("metadata_location"), location0);
        final TableInfo tableInfo1 = TableInfo.builder()
            .name(qualifiedName)
            .metadata(ImmutableMap.of("new_param_key", "new_param_value"))
            .build();
        polarisTableService.update(requestContext, tableInfo1);
        final TableInfo tableResult1 = polarisTableService.get(requestContext, qualifiedName);
        Assert.assertEquals(tableResult1.getMetadata().get("metadata_location"), location0);
    }

    /**
     * Test update passes and params are updated when params are provided, and the location
     * is provided but unchanged.
     */
    @Test
    public void testUpdateTableAcceptOnlyParamsChange() {
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table1");
        final String location0 = "src/test/resources/metadata/00000-9b5d4c36-130c-4288-9599-7d850c203d11.metadata.json";
        final TableInfo tableInfo0 = TableInfo.builder()
            .name(qualifiedName)
            .metadata(ImmutableMap.of("metadata_location", location0))
            .build();
        polarisTableService.create(requestContext, tableInfo0);
        final TableInfo tableResult0 = polarisTableService.get(requestContext, qualifiedName);
        Assert.assertEquals(tableResult0.getMetadata().get("metadata_location"), location0);

        PolarisTableEntity entity = polarisStoreService.getTable(DB_NAME, "table1")
            .orElseThrow(() -> new RuntimeException("Expected table entity to be present"));
        Assert.assertTrue(entity.getParams().isEmpty());

        final TableInfo tableInfo1 = TableInfo.builder()
            .name(qualifiedName)
            .metadata(ImmutableMap.of(
                "previous_metadata_location", location0,
                "metadata_location", location0,
                "new_param_key", "new_param_value"))
            .build();
        polarisTableService.update(requestContext, tableInfo1);
        final TableInfo tableResult1 = polarisTableService.get(requestContext, qualifiedName);
        Assert.assertEquals(tableResult1.getMetadata().get("metadata_location"), location0);
        Assert.assertFalse(tableResult1.getMetadata().containsKey("new_param_key"));

        PolarisTableEntity updatedEntity = polarisStoreService.getTable(DB_NAME, "table1")
            .orElseThrow(() -> new RuntimeException("Expected table entity to be present"));
        Assert.assertTrue(updatedEntity.getParams().get("new_param_key").equals("new_param_value"));
    }
}
