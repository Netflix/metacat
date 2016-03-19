package com.netflix.metacat.converters;

import com.facebook.presto.spi.type.TypeManager;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.TableDto;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;

public interface HiveConverters {
    TableDto hiveToMetacatTable(QualifiedName name, Table table, TypeManager typeManager);

    Database metacatToHiveDatabase(DatabaseDto databaseDto);

    Table metacatToHiveTable(TableDto dto, TypeManager typeManager);

    PartitionDto hiveToMetacatPartition(TableDto tableDto, Partition partition);

    List<String> getPartValsFromName(TableDto tableDto, String partName);

    String getNameFromPartVals(TableDto tableDto, List<String> partVals);

    Partition metacatToHivePartition(PartitionDto partitionDto, TableDto tableDto);
}
