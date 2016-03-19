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
