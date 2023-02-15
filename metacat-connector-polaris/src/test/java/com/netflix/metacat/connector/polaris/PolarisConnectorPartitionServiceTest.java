package com.netflix.metacat.connector.polaris;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest;
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveRequest;
import com.netflix.metacat.common.server.connectors.model.StorageInfo;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableHandler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.same;

public class PolarisConnectorPartitionServiceTest {
    private QualifiedName tableName;
    private QualifiedName partitionName;
    private QualifiedName partitionName2;
    private TableInfo tableInfo;
    private PartitionListRequest partitionsListRequest;
    private PartitionsSaveRequest partitionsSaveRequest;
    private List<String> partitionNames;
    private List<String> uris;
    private PartitionInfo partitionInfo;
    private PartitionInfo partitionInfo2;
    private Sort sort;

    @Mock
    private ConnectorRequestContext requestContext;
    @Mock
    private ConnectorContext connectorContext;
    @Mock
    private IcebergTableHandler icebergTableHandler;
    @Mock
    private PolarisConnectorTableService tableService;

    private PolarisConnectorPartitionService polaris;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        tableName = QualifiedName.fromString("catalog/db/table");
        partitionName = QualifiedName.fromString("catalog/db/table/dateint=20230101");
        partitionName2 = QualifiedName.fromString("catalog/db/table/dateint=20230102");
        tableInfo = TableInfo.builder().name(tableName).build();
        partitionsListRequest = new PartitionListRequest();
        partitionsSaveRequest = new PartitionsSaveRequest();
        partitionNames = Arrays.asList("p1", "p2");
        uris = Arrays.asList("u1", "u2");
        partitionInfo = new PartitionInfo();
        partitionInfo2 = new PartitionInfo();
        sort = new Sort();

        polaris = new PolarisConnectorPartitionService(connectorContext, icebergTableHandler, tableService);
    }

    @Test
    public void getPartitions() {

        partitionsListRequest.setFilter("filter");
        partitionsListRequest.setPartitionNames(Collections.singletonList("dateint=20230101"));
        partitionsListRequest.setPageable(new Pageable(1, 0));
        partitionsListRequest.setSort(sort);

        doReturn(Arrays.asList(partitionInfo, partitionInfo2))
            .when(icebergTableHandler).getPartitions(
                same(tableInfo),
                same(connectorContext),
                eq("filter"),
                eq(Collections.singletonList("dateint=20230101")),
                same(sort));

        final List<PartitionInfo> partitions
            = polaris.getPartitions(requestContext, tableName, partitionsListRequest, tableInfo);

        assertThat(partitions).isEqualTo(Collections.singletonList(partitionInfo));
    }

    @Test
    public void getPartitionKeys() {

        partitionInfo.setName(partitionName);
        partitionInfo2.setName(partitionName2);

        partitionsListRequest.setFilter("filter");
        partitionsListRequest.setPartitionNames(Collections.singletonList("dateint=20230101"));
        partitionsListRequest.setPageable(new Pageable(2, 0));
        partitionsListRequest.setSort(sort);

        doReturn(Arrays.asList(partitionInfo, partitionInfo2))
            .when(icebergTableHandler).getPartitions(
                same(tableInfo),
                same(connectorContext),
                eq("filter"),
                eq(Collections.singletonList("dateint=20230101")),
                same(sort));

        final List<String> partitionKeys
            = polaris.getPartitionKeys(requestContext, tableName, partitionsListRequest, tableInfo);

        assertThat(partitionKeys).isEqualTo(Arrays.asList("dateint=20230101", "dateint=20230102"));
    }

    @Test
    public void get() {
        partitionInfo.setName(partitionName);
        partitionInfo2.setName(partitionName2);

        doReturn(tableInfo).when(tableService)
            .get(requestContext, QualifiedName.ofTable("catalog", "db", "table"));

        doReturn(Arrays.asList(partitionInfo, partitionInfo2))
            .when(icebergTableHandler).getPartitions(
                same(tableInfo),
                same(connectorContext),
                eq(null),
                eq(Collections.singletonList("dateint=20230101")),
                eq(null));

        final PartitionInfo partition = polaris.get(requestContext, partitionName);

        assertThat(partition).isSameAs(partitionInfo);
    }

    @Test
    public void getPartitionNames() {
        assertThatThrownBy(() -> polaris.getPartitionNames(requestContext, uris, true))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void getPartitionUris() {
        partitionInfo.setName(partitionName);
        partitionInfo2.setName(partitionName2);

        partitionInfo.setSerde(StorageInfo.builder().uri("uri1").build());
        partitionInfo2.setSerde(StorageInfo.builder().uri("uri2").build());

        doReturn(Arrays.asList(partitionInfo, partitionInfo2))
            .when(icebergTableHandler).getPartitions(
                same(tableInfo),
                same(connectorContext),
                eq("filter"),
                eq(Collections.singletonList("dateint=20230101")),
                same(sort));

        partitionsListRequest.setFilter("filter");
        partitionsListRequest.setPartitionNames(Collections.singletonList("dateint=20230101"));
        partitionsListRequest.setPageable(new Pageable(1, 1));
        partitionsListRequest.setSort(sort);

        final List<String> partitionUris
            = polaris.getPartitionUris(requestContext, tableName, partitionsListRequest, tableInfo);

        assertThat(partitionUris).isEqualTo(Collections.singletonList("uri2"));
    }

    @Test
    public void getPartitionCount() {
        doReturn(Arrays.asList(partitionInfo, partitionInfo2))
            .when(icebergTableHandler).getPartitions(
                same(tableInfo),
                same(connectorContext),
                eq(null),
                eq(null),
                eq(null));

        assertThat(polaris.getPartitionCount(requestContext, tableName, tableInfo)).isEqualTo(2);
    }

    @Test
    public void create() {
        assertThatThrownBy(() -> polaris.create(requestContext, partitionInfo))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void update() {
        assertThatThrownBy(() -> polaris.update(requestContext, partitionInfo))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void delete() {
        assertThatThrownBy(() -> polaris.delete(requestContext, partitionName))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void savePartitions() {
        assertThatThrownBy(() -> polaris.savePartitions(requestContext, tableName, partitionsSaveRequest))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void deletePartitions() {
        assertThatThrownBy(() -> polaris.deletePartitions(requestContext, tableName, partitionNames, tableInfo))
            .isInstanceOf(UnsupportedOperationException.class);
    }
}
