package com.netflix.metacat.converters;

import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.type.TypeManager;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.TableDto;

public interface PrestoConverters {
    TableMetadata fromTableDto(QualifiedName name, TableDto table, TypeManager typeManager);

    QualifiedTableName getQualifiedTableName(QualifiedName name);

    PartitionDto toPartitionDto(QualifiedName name, ConnectorPartition partition);

    QualifiedName toQualifiedName(QualifiedTableName qualifiedTableName);

    TableDto toTableDto(QualifiedName name, String type, TableMetadata ptm);

    ConnectorPartition fromPartitionDto(PartitionDto partitionDto);
}
