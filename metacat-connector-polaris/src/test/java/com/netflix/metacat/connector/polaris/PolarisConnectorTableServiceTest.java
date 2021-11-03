
package com.netflix.metacat.connector.polaris;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.dto.SortOrder;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter;
import com.netflix.metacat.connector.polaris.configs.PolarisPersistenceConfig;
import com.netflix.metacat.connector.polaris.store.PolarisStoreConnector;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import spock.lang.Shared;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * Test PolarisConnectorTableService.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {PolarisPersistenceConfig.class})
@ActiveProfiles(profiles = {"polarisconnectortest"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@AutoConfigureDataJpa
public class PolarisConnectorTableServiceTest {
    private static final String CATALOG_NAME = "catalog_name";
    private static final String DB_NAME = "db_name";
    private static final QualifiedName DB_QUALIFIED_NAME = QualifiedName.ofDatabase(CATALOG_NAME, DB_NAME);

    @Autowired
    private PolarisStoreConnector polarisConnector;

    @Shared
    private PolarisConnectorDatabaseService polarisDBService = Mockito.mock(PolarisConnectorDatabaseService.class);

    @Shared
    private ConnectorRequestContext requestContext = new ConnectorRequestContext();

    @Shared
    private ConnectorContext connectorContext = Mockito.mock(ConnectorContext.class);

    /**
     * Test empty list tables.
     */
    @Test
    public void testListTablesEmpty() {
        final PolarisConnectorTableService polarisConnectorTableService = new PolarisConnectorTableService(
            polarisConnector,
            CATALOG_NAME,
            polarisDBService,
            new HiveConnectorInfoConverter(new HiveTypeConverter()),
            connectorContext
        );
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "");
        final List<QualifiedName> names = polarisConnectorTableService.listNames(
            requestContext, DB_QUALIFIED_NAME, qualifiedName, new Sort(null, SortOrder.ASC), new Pageable(2, 0));
        Assert.assertEquals(names, Arrays.asList());
    }

    /**
     * Test table creation then list tables.
     */
    @Test
    public void testTableCreationAndList() {
        final PolarisConnectorTableService polarisConnectorTableService = new PolarisConnectorTableService(
            polarisConnector,
            CATALOG_NAME,
            polarisDBService,
            new HiveConnectorInfoConverter(new HiveTypeConverter()),
            connectorContext
        );
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table1");
        final TableInfo tableInfo = TableInfo.builder().name(qualifiedName).build();
        polarisConnectorTableService.create(requestContext, tableInfo);
        final List<QualifiedName> names = polarisConnectorTableService.listNames(
            requestContext, DB_QUALIFIED_NAME, qualifiedName, new Sort(null, SortOrder.ASC), new Pageable(2, 0));
        Assert.assertEquals(names, Arrays.asList(qualifiedName));
    }

    /**
     * Test multiple table creation then list tables.
     */
    @Test
    public void testMultipleTableCreationAndList() {
        final PolarisConnectorTableService polarisConnectorTableService = new PolarisConnectorTableService(
            polarisConnector,
            CATALOG_NAME,
            polarisDBService,
            new HiveConnectorInfoConverter(new HiveTypeConverter()),
            connectorContext
        );
        final List<QualifiedName> createdTables = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table" + i);
            final TableInfo tableInfo = TableInfo.builder().name(qualifiedName).build();
            polarisConnectorTableService.create(requestContext, tableInfo);
            createdTables.add(qualifiedName);
        }
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "");
        final List<QualifiedName> names = polarisConnectorTableService.listNames(
            requestContext, DB_QUALIFIED_NAME, qualifiedName, new Sort(null, SortOrder.ASC), new Pageable(20, 0));
        Assert.assertEquals(names, createdTables);
    }
}

