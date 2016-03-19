package com.netflix.metacat.client.module;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import feign.Response;
import feign.codec.Decoder;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;

public class JacksonDecoder implements Decoder {
    private final ObjectMapper mapper;

    public JacksonDecoder(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Object decode(Response response, Type type) throws IOException {
        if (response.body() == null || response.status() == 204
                || (response.body().length() != null && response.body().length() == 0)) {
            return null;
        }
        InputStream inputStream = response.body().asInputStream();
        try {
            return mapper.readValue(inputStream, mapper.constructType(type));
        } catch (RuntimeJsonMappingException e) {
            if (e.getCause() != null && e.getCause() instanceof IOException) {
                throw IOException.class.cast(e.getCause());
            }
            throw e;
        }
    }
}
