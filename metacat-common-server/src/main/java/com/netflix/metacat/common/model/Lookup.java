package com.netflix.metacat.common.model;

import com.netflix.metacat.common.server.Config;

import javax.inject.Inject;
import java.util.Date;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by amajumdar on 6/30/15.
 */
public class Lookup {
    private static Config config;
    private Long id;
    private String name;
    private String type = "string";
    private Set<String> values;
    private Date dateCreated;
    private Date lastUpdated;
    private String createdBy;
    private String lastUpdatedBy;

    public Lookup() {
        checkNotNull(config, "config should have been set in the static setConfig");
        createdBy = lastUpdatedBy = config.getLookupServiceUserAdmin();
    }

    /**
     * This must be called statically to set the config before the class can be used.
     *
     * @param config the metacat configuration
     */
    @Inject
    public static void setConfig(Config config) {
        Lookup.config = config;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Set<String> getValues() {
        return values;
    }

    public void setValues(Set<String> values) {
        this.values = values;
    }

    public Date getDateCreated() {
        return dateCreated;
    }

    public void setDateCreated(Date dateCreated) {
        this.dateCreated = dateCreated;
    }

    public Date getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
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
}
