package com.netflix.metacat.common.dto;

import java.util.List;

/**
 * Created by amajumdar on 5/28/15.
 */
public class GetPartitionsRequestDto extends BaseDto{
    String filter;
    List<String> partitionNames;
    Boolean includePartitionDetails = false;

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public void setPartitionNames(List<String> partitionNames) {
        this.partitionNames = partitionNames;
    }

    public Boolean getIncludePartitionDetails() {
        return includePartitionDetails!=null?includePartitionDetails:false;
    }

    public void setIncludePartitionDetails(Boolean includePartitionDetails) {
        this.includePartitionDetails = includePartitionDetails;
    }
}
