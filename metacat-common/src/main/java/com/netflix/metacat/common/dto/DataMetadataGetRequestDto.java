package com.netflix.metacat.common.dto;

/**
 * Created by amajumdar on 6/26/15.
 */
public class DataMetadataGetRequestDto extends BaseDto{
    private String uri;

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }
}
