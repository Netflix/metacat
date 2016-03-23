/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

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
