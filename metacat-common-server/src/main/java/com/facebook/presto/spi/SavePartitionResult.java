package com.facebook.presto.spi;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by amajumdar on 7/20/15.
 */
public class SavePartitionResult {
    List<String> added;
    List<String> updated;

    public SavePartitionResult() {
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
