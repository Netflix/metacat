package com.netflix.metacat.main.services.search;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.json.MetacatJsonLocator;
import org.elasticsearch.action.get.GetResponse;

import java.util.Map;

/**
 * Created by amajumdar on 8/17/15.
 */
public class ElasticSearchDoc {
    protected interface Field {
        public static final String USER = "user_";
        public static final String DELETED = "deleted_";
        public static final String REFRESH_MARKER = "refreshMarker_";

    }
    public enum Type { catalog(CatalogDto.class), database(DatabaseDto.class), table(TableDto.class), mview(TableDto.class), partition(PartitionDto.class);
        Class clazz;
        Type(Class clazz) {
           this.clazz = clazz;
        }

        public Class getClazz() {
            return clazz;
        }
    }
    String id;
    Object dto;
    String user;
    boolean deleted;
    String refreshMarker;

    public ElasticSearchDoc(String id, Object dto, String user, boolean deleted) {
        this.id = id;
        this.dto = dto;
        this.user = user;
        this.deleted = deleted;
    }

    public ElasticSearchDoc(String id, Object dto, String user, boolean deleted, String refreshMarker){
        this.id = id;
        this.dto = dto;
        this.user = user;
        this.deleted = deleted;
        this.refreshMarker = refreshMarker;
    }

    private static Class getClass(String type){
        return Type.valueOf(type).getClazz();
    }

    public ObjectNode toJsonObject(){
        ObjectNode oMetadata = MetacatJsonLocator.INSTANCE.toJsonObject(dto);
        //True if this entity has been deleted
        oMetadata.put(Field.DELETED, deleted);
        //True if this entity has been deleted
        oMetadata.put(Field.USER, user);
        if( refreshMarker != null){
            oMetadata.put(Field.REFRESH_MARKER, refreshMarker);
        }
        return oMetadata;
    }

    public String toJsonString(){
        String result = MetacatJsonLocator.INSTANCE.toJsonString(toJsonObject());
        return result.replace("{}", "null");
    }

    public static ElasticSearchDoc parse(GetResponse response){
        ElasticSearchDoc result = null;
        if(response.isExists() ){
            Map<String,Object> responseMap = response.getSourceAsMap();
            String user = (String) responseMap.get(Field.USER);
            boolean deleted = (boolean) responseMap.get(Field.DELETED);
            Object dto = MetacatJsonLocator.INSTANCE.parseJsonValue(response.getSourceAsBytes(), getClass(
                    response.getType()));
            result = new ElasticSearchDoc( response.getId(), dto, user, deleted);
        }
        return result;
    }

    public Object getDto() {
        return dto;
    }

    public String getUser() {
        return user;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public String getId(){
        return id;
    }
}
