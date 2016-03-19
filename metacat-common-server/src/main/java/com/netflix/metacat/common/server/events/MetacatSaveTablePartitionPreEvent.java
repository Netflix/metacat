package com.netflix.metacat.common.server.events;

import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.PartitionDto;

import java.util.List;
import java.util.Objects;

public class MetacatSaveTablePartitionPreEvent extends MetacatEvent {
    private final List<PartitionDto> partitions;

    public MetacatSaveTablePartitionPreEvent(QualifiedName name, List<PartitionDto> partitions, MetacatContext metacatContext) {
        super(name, metacatContext);
        this.partitions = partitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetacatSaveTablePartitionPreEvent)) return false;
        if (!super.equals(o)) return false;
        MetacatSaveTablePartitionPreEvent that = (MetacatSaveTablePartitionPreEvent) o;
        return Objects.equals(partitions, that.partitions);
    }

    public List<PartitionDto> getPartitions() {
        return partitions;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(partitions);
    }

    @Override
    public String toString() {
        return "MetacatSaveTablePartitionPreEvent{" +
                "partitions=" + partitions +
                '}';
    }
}
