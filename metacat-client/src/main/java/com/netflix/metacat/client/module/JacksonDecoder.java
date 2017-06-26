/*
 *
 *  Copyright 2016 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.metacat.client.module;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import feign.Response;
import feign.codec.Decoder;
import lombok.NonNull;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;

/**
 * Decoder for Metacat response.
 *
 * @author amajumdar
 */
public class JacksonDecoder implements Decoder {
    private final ObjectMapper mapper;

    /**
     * Constructor.
     *
     * @param mapper Jackson mapper for Metacat response.
     */
    public JacksonDecoder(@Nonnull @NonNull final ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object decode(final Response response, final Type type) throws IOException {
        if (
            response.body() == null
                || response.status() == 204
                || (response.body().length() != null && response.body().length() == 0)
            ) {
            return null;
        }
        final InputStream inputStream = response.body().asInputStream();
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
