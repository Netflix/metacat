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

package com.facebook.presto.spi;

/**
 * Created by amajumdar on 3/16/15.
 */
public class Sort {
    private String sortBy;
    private SortOrder order;

    public Sort() {
    }

    public Sort(String sortBy, SortOrder order) {
        this.sortBy = sortBy;
        this.order = order;
    }

    public String getSortBy() {
        return sortBy;
    }

    public void setSortBy(String sortBy) {
        this.sortBy = sortBy;
    }

    public SortOrder getOrder() {
        return order==null?SortOrder.ASC:order;
    }

    public void setOrder(SortOrder order) {
        this.order = order;
    }

    public boolean hasSort(){
        return sortBy != null;
    }
}
