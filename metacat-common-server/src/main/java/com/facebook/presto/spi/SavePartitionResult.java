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
