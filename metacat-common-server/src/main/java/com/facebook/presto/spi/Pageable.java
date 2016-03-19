package com.facebook.presto.spi;

/**
 * Represents the pagination information
 * Created by amajumdar on 3/16/15.
 */
public class Pageable {
    private Integer limit;
    private Integer offset;

    public Pageable() {
    }

    public Pageable(Integer limit, Integer offset) {
        this.limit = limit;
        this.offset = offset;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public Integer getOffset() {
        return offset==null?0:offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public boolean isPageable(){
        return limit != null;
    }
}
