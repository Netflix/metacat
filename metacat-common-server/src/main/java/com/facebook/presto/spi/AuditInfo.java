package com.facebook.presto.spi;

/**
 * Created by amajumdar on 3/3/15.
 */
public class AuditInfo {
    private String createdBy;
    private String lastUpdatedBy;
    private Long createdDate;
    private Long lastUpdatedDate;

    public AuditInfo() {
    }

    public AuditInfo(String createdBy, String lastUpdatedBy, Long createdDate, Long lastUpdatedDate) {
        this.createdBy = createdBy;
        this.lastUpdatedBy = lastUpdatedBy;
        this.createdDate = createdDate;
        this.lastUpdatedDate = lastUpdatedDate;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getLastUpdatedBy() {
        return lastUpdatedBy;
    }

    public void setLastUpdatedBy(String lastUpdatedBy) {
        this.lastUpdatedBy = lastUpdatedBy;
    }

    public Long getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(Long createdDate) {
        this.createdDate = createdDate;
    }

    public Long getLastUpdatedDate() {
        return lastUpdatedDate;
    }

    public void setLastUpdatedDate(Long lastUpdatedDate) {
        this.lastUpdatedDate = lastUpdatedDate;
    }
}
