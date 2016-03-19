package com.netflix.metacat.common.dto;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by amajumdar on 5/28/15.
 */
public class PartitionsSaveResponseDto extends BaseDto{
    List<String> added;
    List<String> updated;

    public PartitionsSaveResponseDto() {
        added = new ArrayList<>();
        updated = new ArrayList<>();
    }

    public List<String> getAdded() {
        return added;
    }

    public void setAdded(List<String> added) {
        if( added != null) {
            this.added = added;
        }
    }

    public List<String> getUpdated() {
        return updated;
    }

    public void setUpdated(List<String> updated) {
        if( updated != null) {
            this.updated = updated;
        }
    }
}
