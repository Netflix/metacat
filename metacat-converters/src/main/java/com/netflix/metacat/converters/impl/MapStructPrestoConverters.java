package com.netflix.metacat.converters.impl;

import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.AuditInfo;
import com.facebook.presto.spi.ColumnDetailMetadata;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionDetail;
import com.facebook.presto.spi.ConnectorPartitionDetailImpl;
import com.facebook.presto.spi.ConnectorTableDetailMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StorageInfo;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.AuditDto;
import com.netflix.metacat.common.dto.FieldDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.StorageDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.converters.PrestoConverters;
import com.netflix.metacat.converters.TypeConverter;
import org.mapstruct.InheritInverseConfiguration;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

import javax.inject.Provider;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

@Mapper(uses = DateConverters.class,
        unmappedTargetPolicy = ReportingPolicy.ERROR,
        componentModel = "default")
public abstract class MapStructPrestoConverters implements PrestoConverters {
    Provider<TypeConverter> typeConverter;
    public void setTypeConverter(Provider<TypeConverter> typeConverter){
        this.typeConverter = typeConverter;
    }

    private List<ColumnMetadata> columnsFromTable(TableDto table, TypeManager typeManager) {
        if (table.getFields() == null) {
            return Lists.newArrayList();
        }

        return table.getFields().stream()
                .map(fieldDto -> fromFieldDto(fieldDto, typeManager))
                .collect(Collectors.toList());
    }

    @InheritInverseConfiguration
    protected abstract AuditInfo fromAuditDto(AuditDto audit);

    protected ColumnMetadata fromFieldDto(FieldDto fieldDto, TypeManager typeManager) {
        String type = fieldDto.getType();
        if( type == null){
            type = fieldDto.getSource_type();
        }
        return new ColumnMetadata(
                fieldDto.getName(),
                typeConverter.get().toType(type, typeManager),
                fieldDto.isPartition_key(),
                fieldDto.getComment(),
                false
        );
    }

    @InheritInverseConfiguration
    protected abstract StorageInfo fromStorageDto(StorageDto serde);

    @Override
    public TableMetadata fromTableDto(QualifiedName name, TableDto table, TypeManager typeManager) {
        checkNotNull(name, "name is null");
        checkNotNull(table, "table is null");
        checkNotNull(typeManager, "typeManager is null");

        return new TableMetadata(name.getCatalogName(), fromTableDto(table, typeManager));
    }

    protected ConnectorTableMetadata fromTableDto(TableDto table, TypeManager typeManager) {
        return new ConnectorTableDetailMetadata(
                toSchemaTableName(table.getName()),
                columnsFromTable(table, typeManager),
                table.getSerde()==null?null:table.getSerde().getOwner(),
                fromStorageDto(table.getSerde()),
                table.getMetadata(),
                fromAuditDto(table.getAudit())
        );
    }

    @Override
    public QualifiedTableName getQualifiedTableName(QualifiedName name) {
        return new QualifiedTableName(name.getCatalogName(), name.getDatabaseName(), name.getTableName());
    }

    protected AuditDto toAuditDto(ConnectorTableMetadata connectorTableMetadata) {
        if (connectorTableMetadata != null && connectorTableMetadata instanceof ConnectorTableDetailMetadata) {
            return toAuditDto(((ConnectorTableDetailMetadata) connectorTableMetadata).getAuditInfo());
        }

        return null;
    }

    protected AuditDto toAuditDto(ConnectorPartition connectorPartition) {
        if (connectorPartition != null && connectorPartition instanceof ConnectorPartitionDetail) {
            return toAuditDto(((ConnectorPartitionDetail) connectorPartition).getAuditInfo());
        }

        return null;
    }

    @Mapping(target = "createdBy", source = "createdBy")
    @Mapping(target = "createdDate", source = "createdDate")
    @Mapping(target = "lastModifiedBy", source = "lastUpdatedBy")
    @Mapping(target = "lastModifiedDate", source = "lastUpdatedDate")
    protected abstract AuditDto toAuditDto(AuditInfo auditInfo);

    @Mapping(target = "name", source = "name")
    @Mapping(target = "type", source = "type")
    @Mapping(target = "partition_key", source = "partitionKey")
    @Mapping(target = "comment", source = "comment")
    @Mapping(target = "source_type", ignore = true)
    @Mapping(target = "pos", ignore = true)
    @Mapping(target = "isNullable", ignore = true)
    @Mapping(target = "size", ignore = true)
    @Mapping(target = "defaultValue", ignore = true)
    @Mapping(target = "isSortKey", ignore = true)
    @Mapping(target = "isIndexKey", ignore = true)
    protected abstract FieldDto toFieldDto(ColumnMetadata column);

    protected List<FieldDto> toFieldDtos(List<ColumnMetadata> columns){
        List<FieldDto> result = Lists.newArrayList();
        if( columns != null ){
            for(int i=0; i < columns.size(); i++){
                ColumnMetadata column = columns.get(i);
                FieldDto fieldDto = toFieldDto(column);
                if( column instanceof ColumnDetailMetadata){
                    ColumnDetailMetadata columnDetail = (ColumnDetailMetadata) column;
                    fieldDto.setSource_type(columnDetail.getSourceType());
                    fieldDto.setIsNullable(columnDetail.getIsNullable());
                    fieldDto.setSize(columnDetail.getSize());
                    fieldDto.setDefaultValue(columnDetail.getDefaultValue());
                    fieldDto.setIsIndexKey(columnDetail.getIsIndexKey());
                    fieldDto.setIsSortKey(columnDetail.getIsSortKey());
                }
                fieldDto.setPos(i);
                result.add(fieldDto);
            }
        }
        return result;
    }

    protected Map<String, String> toMetadata(ConnectorTableMetadata metadata) {
        if (metadata != null && metadata instanceof ConnectorTableDetailMetadata) {
            ConnectorTableDetailMetadata detailMetadata = (ConnectorTableDetailMetadata) metadata;
            return detailMetadata.getMetadata();
        }

        return null;
    }

    protected Map<String, String> toMetadata(ConnectorPartition partition) {
        if (partition != null && partition instanceof ConnectorPartitionDetail) {
            ConnectorPartitionDetail partitionDetail = (ConnectorPartitionDetail) partition;
            return partitionDetail.getMetadata();
        }

        return null;
    }

    @Mapping(target = "name", source = "name")
    @Mapping(target = "audit", source = "partition")
    @Mapping(target = "serde", source = "partition")
    @Mapping(target = "metadata", source = "partition")
    @Mapping(target = "dataExternal", ignore = true)
    @Mapping(target = "dataMetadata", ignore = true)
    @Mapping(target = "definitionMetadata", ignore = true)
    @Override
    public abstract PartitionDto toPartitionDto(QualifiedName name, ConnectorPartition partition);

    protected List<String> toPartitionKeys(List<ColumnMetadata> columns) {
        return columns.stream()
                .filter(ColumnMetadata::isPartitionKey)
                .map(ColumnMetadata::getName)
                .collect(Collectors.toList());
    }

    @Override
    public QualifiedName toQualifiedName(QualifiedTableName qualifiedTableName) {
        return QualifiedName.ofTable(qualifiedTableName.getCatalogName(), qualifiedTableName.getSchemaName(),
                qualifiedTableName.getTableName());
    }

    public SchemaTableName toSchemaTableName(QualifiedName name) {
        return new SchemaTableName(name.getDatabaseName(), name.getTableName());
    }

    @Mapping(target = "owner", ignore = true)
    @Mapping(target = "parameters", source = "parameters")
    @Mapping(target = "serdeInfoParameters", source = "serdeInfoParameters")
    @Mapping(target = "serializationLib", source = "serializationLib")
    @Mapping(target = "inputFormat", source = "inputFormat")
    @Mapping(target = "outputFormat", source = "outputFormat")
    @Mapping(target = "uri", source = "uri")
    protected abstract StorageDto toStorageDto(StorageInfo storageInfo);

    protected StorageDto toStorageDto(ConnectorTableMetadata connectorTableMetadata) {
        if (connectorTableMetadata != null && connectorTableMetadata instanceof ConnectorTableDetailMetadata) {
            ConnectorTableDetailMetadata detailMetadata = (ConnectorTableDetailMetadata) connectorTableMetadata;
            StorageDto storageDto = toStorageDto(detailMetadata.getStorageInfo());
            storageDto.setOwner(detailMetadata.getOwner());
            return storageDto;
        }

        return null;
    }

    protected StorageDto toStorageDto(ConnectorPartition connectorPartition) {
        if (connectorPartition != null && connectorPartition instanceof ConnectorPartitionDetail) {
            ConnectorPartitionDetail detailMetadata = (ConnectorPartitionDetail) connectorPartition;
            StorageDto storageDto = toStorageDto(detailMetadata.getStorageInfo());
            if(detailMetadata.getAuditInfo() != null) {
                storageDto.setOwner(detailMetadata.getAuditInfo().getCreatedBy());
            }
            return storageDto;
        }

        return null;
    }

    protected String toString(Type type) {
        return typeConverter.get().fromType(type);
    }

    @Mapping(target = "name", source = "name")
    @Mapping(target = "metadata", source = "ptm.metadata")
    @Mapping(target = "dataExternal", ignore = true)
    @Mapping(target = "dataMetadata", ignore = true)
    @Mapping(target = "definitionMetadata", ignore = true)
    @Mapping(target = "audit", source = "ptm.metadata")
    @Mapping(target = "partition_keys", source = "ptm.columns")
    @Mapping(target = "serde", source = "ptm.metadata")
    @Mapping(target = "fields", source = "ptm.columns")
    @Override
    public abstract TableDto toTableDto(QualifiedName name, String type, TableMetadata ptm);

    @Override
    public ConnectorPartition fromPartitionDto(PartitionDto partitionDto) {
        return new ConnectorPartitionDetailImpl(partitionDto.getName().getPartitionName(), TupleDomain.none(), fromStorageDto(partitionDto.getSerde()), partitionDto.getMetadata(), fromAuditDto(partitionDto.getAudit()));
    }
}
