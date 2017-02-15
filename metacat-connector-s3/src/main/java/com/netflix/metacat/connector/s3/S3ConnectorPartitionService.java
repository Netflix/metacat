/*
 *
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.metacat.connector.s3;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.persist.Transactional;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.partition.parser.PartitionParser;
import com.netflix.metacat.common.partition.util.FilterPartition;
import com.netflix.metacat.common.partition.util.PartitionUtil;
import com.netflix.metacat.common.partition.visitor.PartitionKeyParserEval;
import com.netflix.metacat.common.partition.visitor.PartitionParamParserEval;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.model.BaseInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest;
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveRequest;
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveResponse;
import com.netflix.metacat.common.server.exception.PartitionAlreadyExistsException;
import com.netflix.metacat.common.server.exception.PartitionNotFoundException;
import com.netflix.metacat.common.server.exception.TableNotFoundException;
import com.netflix.metacat.connector.s3.dao.PartitionDao;
import com.netflix.metacat.connector.s3.dao.TableDao;
import com.netflix.metacat.connector.s3.model.Partition;
import com.netflix.metacat.connector.s3.model.Table;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.StringReader;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * S3 Connector implementation for partitions.
 *
 * @author amajumdar
 */
@Transactional
@Slf4j
public class S3ConnectorPartitionService implements ConnectorPartitionService {
    private static final String FIELD_DATE_CREATED = "dateCreated";
    private static final String FIELD_BATCHID = "batchid";
    private final TableDao tableDao;
    private final PartitionDao partitionDao;
    private final S3ConnectorInfoConverter infoConverter;
    private final String catalogName;

    /**
     * Constructor.
     * @param catalogName catalog name
     * @param tableDao table DAO impl
     * @param partitionDao partition DAO impl
     * @param infoConverter Converter for the S3 resources
     */
    @Inject
    public S3ConnectorPartitionService(@Named("catalogName") final String catalogName, final TableDao tableDao,
        final PartitionDao partitionDao, final S3ConnectorInfoConverter infoConverter) {
        this.tableDao = tableDao;
        this.partitionDao = partitionDao;
        this.infoConverter = infoConverter;
        this.catalogName = catalogName;
    }

    @Override
    public void create(@Nonnull final ConnectorContext context, @Nonnull final PartitionInfo partitionInfo) {
        final QualifiedName name = partitionInfo.getName();
        log.debug("Start: Create partition {}", name);
        final QualifiedName tableName = QualifiedName.ofTable(catalogName, name.getDatabaseName(),
            name.getTableName());
        // Table
        final Table table = getTable(tableName);
        final List<Partition> partitions = partitionDao
            .getPartitions(table.getId(), Lists.newArrayList(name.getPartitionName()), null, null, null, null);
        if (!partitions.isEmpty()) {
            throw new PartitionAlreadyExistsException(tableName, name.getPartitionName());
        }
        partitionDao.save(infoConverter.toPartition(table, partitionInfo));
        log.debug("End: Create partition {}", name);
    }

    private Table getTable(final QualifiedName tableName) {
        final Table result = tableDao.getBySourceDatabaseTableName(catalogName, tableName.getDatabaseName(),
            tableName.getTableName());
        if (result == null) {
            throw new TableNotFoundException(tableName);
        }
        return result;
    }

    @Override
    public List<PartitionInfo> getPartitions(@Nonnull final ConnectorContext context,
        @Nonnull final QualifiedName tableName, @Nonnull final PartitionListRequest partitionsRequest) {
        log.debug("Get partitions for table {}", tableName);
        return _getPartitions(tableName, partitionsRequest.getFilter(), partitionsRequest.getPartitionNames(),
            partitionsRequest.getSort(), partitionsRequest.getPageable(), true);
    }

    @Override
    public void update(@Nonnull final ConnectorContext context, @Nonnull final PartitionInfo partitionInfo) {
        final QualifiedName name = partitionInfo.getName();
        log.debug("Start: Update partition {}", name);
        final QualifiedName tableName = QualifiedName.ofTable(catalogName, name.getDatabaseName(),
            name.getTableName());
        // Table
        final Table table = getTable(tableName);
        final List<Partition> partitions = partitionDao
            .getPartitions(table.getId(), Lists.newArrayList(name.getPartitionName()), null, null, null, null);
        if (partitions.isEmpty()) {
            throw new PartitionNotFoundException(tableName, name.getPartitionName());
        }
        partitionDao.save(infoConverter.fromPartitionInfo(partitionInfo));
        log.debug("End: Update partition {}", name);
    }

    @Override
    public void delete(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName name) {
        log.debug("Start: Delete partition {}", name);
        partitionDao.deleteByNames(catalogName, name.getDatabaseName(), name.getTableName(),
            Lists.newArrayList(name.getPartitionName()));
        log.debug("End: Delete partition {}", name);
    }

    @Override
    public PartitionsSaveResponse savePartitions(@Nonnull final ConnectorContext context,
        @Nonnull final QualifiedName tableName, @Nonnull final PartitionsSaveRequest partitionsSaveRequest) {
        log.debug("Start: Save partitions for table {}", tableName);
        // Table
        final Table table = getTable(tableName);

        // New partition ids
        final List<String> addedPartitionIds = Lists.newArrayList();
        // Updated partition ids
        final List<String> existingPartitionIds = Lists.newArrayList();
        //
        Map<String, Partition> existingPartitionMap = Maps.newHashMap();

        if (partitionsSaveRequest.getCheckIfExists()) {
            final List<String> partitionNames = partitionsSaveRequest.getPartitions().stream().map(
                partition -> {
                    final String partitionName = partition.getName().getPartitionName();
                    PartitionUtil.validatePartitionName(partitionName, infoConverter.partitionKeys(table));
                    return partitionName;
                }).collect(Collectors.toList());
            existingPartitionMap = getPartitionsByNames(table.getId(), partitionNames);
        }

        // New partitions
        final List<Partition> s3Partitions = Lists.newArrayList();
        for (PartitionInfo partition : partitionsSaveRequest.getPartitions()) {
            final String partitionName = partition.getName().getPartitionName();
            final Partition s3Partition = existingPartitionMap.get(partitionName);
            if (s3Partition == null) {
                addedPartitionIds.add(partitionName);
                s3Partitions.add(infoConverter.toPartition(table, partition));
            } else {
                final String partitionUri = infoConverter.getUri(partition);
                final String s3PartitionUri = s3Partition.getUri();
                if (partitionUri != null && !partitionUri.equals(s3PartitionUri)) {
                    s3Partition.setUri(partitionUri);
                    existingPartitionIds.add(partitionName);
                    s3Partitions.add(s3Partition);
                }
            }
        }
        final List<String> partitionIdsForDeletes = partitionsSaveRequest.getPartitionIdsForDeletes();
        if (partitionIdsForDeletes != null && !partitionIdsForDeletes.isEmpty()) {
            partitionDao.deleteByNames(catalogName, tableName.getDatabaseName(), tableName.getTableName(),
                partitionIdsForDeletes);
        }
        partitionDao.save(s3Partitions);
        log.debug("End: Save partitions for table {}", tableName);
        return PartitionsSaveResponse.builder().added(addedPartitionIds).updated(existingPartitionIds).build();
    }

    private Map<String, Partition> getPartitionsByNames(final Long tableId,
        final List<String> partitionNames) {
        final List<Partition> partitions = partitionDao.getPartitions(tableId, partitionNames, null, null, null, null);
        return partitions.stream().collect(Collectors.toMap(Partition::getName, partition -> partition));
    }

    @Override
    public PartitionInfo get(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName name) {
        final QualifiedName tableName = QualifiedName.ofTable(catalogName, name.getDatabaseName(), name.getTableName());
        final Table table = getTable(tableName);
        final List<Partition> partitions = partitionDao
            .getPartitions(table.getId(), Lists.newArrayList(name.getPartitionName()), null, null, null, null);
        if (partitions.isEmpty()) {
            throw new PartitionNotFoundException(tableName, name.getPartitionName());
        }
        log.debug("Get partition for table {}", tableName);
        return infoConverter.toPartitionInfo(tableName, table, partitions.get(0));
    }

    @Override
    public void deletePartitions(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName tableName,
        @Nonnull final List<String> partitionNames) {
        log.debug("Start: Delete partitions {} for table {}", partitionNames, tableName);
        partitionDao.deleteByNames(catalogName, tableName.getDatabaseName(), tableName.getTableName(), partitionNames);
        log.debug("End: Delete partitions {} for table {}", partitionNames, tableName);
    }

    @Override
    public boolean exists(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName name) {
        boolean result = false;
        final Table table = tableDao.getBySourceDatabaseTableName(catalogName, name.getDatabaseName(),
            name.getTableName());
        if (table != null) {
            result = !partitionDao.getPartitions(table.getId(),
                Lists.newArrayList(name.getPartitionName()), null, null, null, null).isEmpty();
        }
        return result;
    }

    @Override
    public int getPartitionCount(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName table) {
        return partitionDao.count(catalogName, table.getDatabaseName(), table.getTableName()).intValue();
    }

    @Override
    public List<PartitionInfo> list(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName name,
        @Nullable final QualifiedName prefix, @Nullable final Sort sort, @Nullable final Pageable pageable) {
        log.debug("Get partitions for table {} with name prefix {}", name, prefix);
        return _getPartitions(name, null, null, sort, pageable, true).stream()
            .filter(p -> p.getName().getPartitionName().startsWith(prefix.getPartitionName()))
            .collect(Collectors.toList());
    }

    @Override
    public Map<String, List<QualifiedName>> getPartitionNames(@Nonnull final ConnectorContext context,
        @Nonnull final List<String> uris, final boolean prefixSearch) {
        return partitionDao.getByUris(uris, prefixSearch).stream().collect(Collectors.groupingBy(Partition::getUri,
            Collectors.mapping(p -> QualifiedName.ofPartition(
                catalogName, p.getTable().getDatabase().getName(), p.getTable().getName(), p.getName()),
                Collectors.toList())));
    }

    @Override
    public List<String> getPartitionKeys(@Nonnull final ConnectorContext context,
        @Nonnull final QualifiedName tableName, @Nonnull final PartitionListRequest partitionsRequest) {
        log.debug("Get partition keys for table {}", tableName);
        return _getPartitions(tableName, partitionsRequest.getFilter(), partitionsRequest.getPartitionNames(),
            partitionsRequest.getSort(), partitionsRequest.getPageable(), true).stream()
            .map(p -> p.getName().getPartitionName()).collect(Collectors.toList());
    }

    @Override
    public List<QualifiedName> listNames(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName name,
        @Nullable final QualifiedName prefix, @Nullable final Sort sort, @Nullable final Pageable pageable) {
        log.debug("Get partition names for table {} with prefix {}", name, prefix);
        return _getPartitions(name, null, null, sort, pageable, true).stream().map(BaseInfo::getName)
            .filter(partitionName -> partitionName.getPartitionName().startsWith(prefix.getPartitionName()))
            .collect(Collectors.toList());
    }

    @Override
    public List<String> getPartitionUris(@Nonnull final ConnectorContext context,
        @Nonnull final QualifiedName tableName, @Nonnull final PartitionListRequest partitionsRequest) {
        log.debug("Get partition uris for table {}", tableName);
        return _getPartitions(tableName, partitionsRequest.getFilter(), partitionsRequest.getPartitionNames(),
            partitionsRequest.getSort(), partitionsRequest.getPageable(), true).stream()
            .filter(p -> p.getSerde() != null && p.getSerde().getUri() != null)
            .map(p -> p.getSerde().getUri()).collect(Collectors.toList());
    }

    @SuppressWarnings("checkstyle:methodname")
    private List<PartitionInfo> _getPartitions(final QualifiedName tableName, final String filterExpression,
        final List<String> partitionIds, final Sort sort, final Pageable pageable,
        final boolean includePartitionDetails) {
        //
        // Limiting the in clause to 5000 part names because the sql query with the IN clause for part_name(767 bytes)
        // will hit the max sql query length(max_allowed_packet for our RDS) if we use more than 5400 or so
        //
        final List<PartitionInfo> partitions = com.google.common.collect.Lists.newArrayList();
        if (partitionIds != null && partitionIds.size() > 5000) {
            final List<List<String>> subFilterPartitionNamesList = com.google.common.collect.Lists
                .partition(partitionIds, 5000);
            subFilterPartitionNamesList.forEach(
                subPartitionIds -> partitions.addAll(_getConnectorPartitions(tableName, filterExpression,
                    subPartitionIds, sort, pageable, includePartitionDetails)));
        } else {
            partitions.addAll(_getConnectorPartitions(tableName, filterExpression, partitionIds, sort, pageable,
                includePartitionDetails));
        }
        return partitions;
    }

    @SuppressWarnings("checkstyle:methodname")
    private List<PartitionInfo> _getConnectorPartitions(final QualifiedName tableName, final String filterExpression,
        final List<String> partitionIds, final Sort sort, final Pageable pageable,
        final boolean includePartitionDetails) {
        // batch exists
        final boolean isBatched = !Strings.isNullOrEmpty(filterExpression) && filterExpression.contains(FIELD_BATCHID);
        // Support for dateCreated
        final boolean hasDateCreated =
            !Strings.isNullOrEmpty(filterExpression) && filterExpression.contains(FIELD_DATE_CREATED);
        String dateCreatedSqlCriteria = null;
        if (hasDateCreated) {
            dateCreatedSqlCriteria = getDateCreatedSqlCriteria(filterExpression);
        }
        // Table
        final Table table = getTable(tableName);
        final Collection<String> singlePartitionExprs = getSinglePartitionExprs(filterExpression);
        final List<Partition> partitions = partitionDao
            .getPartitions(table.getId(), partitionIds, singlePartitionExprs, dateCreatedSqlCriteria, sort,
                Strings.isNullOrEmpty(filterExpression) ? pageable : null);
        final FilterPartition filter = new FilterPartition();
        List<PartitionInfo> result = partitions.stream().filter(partition -> {
            Map<String, String> values = null;
            if (hasDateCreated) {
                values = Maps.newHashMap();
                values.put(FIELD_DATE_CREATED, (partition.getCreatedDate().getTime() / 1000) + "");
            }
            return Strings.isNullOrEmpty(filterExpression)
                || filter
                .evaluatePartitionExpression(filterExpression, partition.getName(), partition.getUri(), isBatched,
                    values);
        }).map(partition -> infoConverter.toPartitionInfo(tableName, table, partition)).collect(Collectors.toList());
        //
        if (pageable != null && pageable.isPageable() && !Strings.isNullOrEmpty(filterExpression)) {
            int limit = pageable.getOffset() + pageable.getLimit();
            if (result.size() < limit) {
                limit = result.size();
            }
            if (pageable.getOffset() > limit) {
                result = Lists.newArrayList();
            } else {
                result = result.subList(pageable.getOffset(), limit);
            }
        }
        return result;
    }

    private String getDateCreatedSqlCriteria(final String filterExpression) {
        final StringBuilder result = new StringBuilder();
        Collection<String> values = com.google.common.collect.Lists.newArrayList();
        if (!Strings.isNullOrEmpty(filterExpression)) {
            try {
                values = (Collection<String>) new PartitionParser(new StringReader(filterExpression)).filter()
                    .jjtAccept(new PartitionParamParserEval(), null);
            } catch (Throwable ignored) {
                //
            }
        }
        for (String value : values) {
            if (result.length() != 0) {
                result.append(" and ");
            }
            result.append(value.replace("dateCreated", "to_seconds(p.date_created)"));
        }
        return result.toString();
    }

    private Collection<String> getSinglePartitionExprs(final String filterExpression) {
        Collection<String> result = com.google.common.collect.Lists.newArrayList();
        if (!Strings.isNullOrEmpty(filterExpression)) {
            try {
                result = (Collection<String>) new PartitionParser(new StringReader(filterExpression)).filter()
                    .jjtAccept(new PartitionKeyParserEval(), null);
            } catch (Throwable ignored) {
                //
            }
        }
        if (result != null) {
            result = result.stream().filter(s -> !(s.startsWith("batchid=") || s.startsWith("dateCreated="))).collect(
                Collectors.toList());
        }
        return result;
    }
}
