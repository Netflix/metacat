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

package com.netflix.metacat.main.services.search;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.json.MetacatJsonLocator;
import lombok.Getter;
import org.elasticsearch.action.get.GetResponse;

import java.util.Map;

/**
 * @author amajumdar
 */
@Getter
public class ElasticSearchDoc {
    private String id;
    private Object dto;
    private String user;
    private boolean deleted;
    private String refreshMarker;

    public ElasticSearchDoc(String id, Object dto, String user, boolean deleted) {
        this.id = id;
        this.dto = dto;
        this.user = user;
        this.deleted = deleted;
    }

    public ElasticSearchDoc(String id, Object dto, String user, boolean deleted, String refreshMarker) {
        this.id = id;
        this.dto = dto;
        this.user = user;
        this.deleted = deleted;
        this.refreshMarker = refreshMarker;
    }

    private static Class getClass(String type) {
        return Type.valueOf(type).getClazz();
    }

    public static ElasticSearchDoc parse(GetResponse response) {
        ElasticSearchDoc result = null;
        if (response.isExists()) {
            Map<String, Object> responseMap = response.getSourceAsMap();
            String user = (String) responseMap.get(Field.USER);
            boolean deleted = (boolean) responseMap.get(Field.DELETED);
            @SuppressWarnings("unchecked")
            Object dto = MetacatJsonLocator.INSTANCE.parseJsonValue(
                    response.getSourceAsBytes(),
                    getClass(response.getType())
            );
            result = new ElasticSearchDoc(response.getId(), dto, user, deleted);
        }
        return result;
    }

    private ObjectNode toJsonObject() {
        ObjectNode oMetadata = MetacatJsonLocator.INSTANCE.toJsonObject(dto);
        //True if this entity has been deleted
        oMetadata.put(Field.DELETED, deleted);
        //True if this entity has been deleted
        oMetadata.put(Field.USER, user);
        if (refreshMarker != null) {
            oMetadata.put(Field.REFRESH_MARKER, refreshMarker);
        }
        return oMetadata;
    }

    String toJsonString() {
        String result = MetacatJsonLocator.INSTANCE.toJsonString(toJsonObject());
        return result.replace("{}", "null");
    }

    public enum Type {
        catalog(CatalogDto.class),
        database(DatabaseDto.class),
        table(TableDto.class),
        mview(TableDto.class),
        partition(PartitionDto.class);

        private Class clazz;

        Type(Class clazz) {
            this.clazz = clazz;
        }

        public Class getClazz() {
            return clazz;
        }
    }

    protected interface Field {
        String USER = "user_";
        String DELETED = "deleted_";
        String REFRESH_MARKER = "refreshMarker_";
    }
}
