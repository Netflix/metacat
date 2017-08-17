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

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import feign.Response;
import feign.codec.Decoder;
import lombok.NonNull;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;

/**
 * Decoder for Metacat response.
 *
 * @author amajumdar
 */
public class JacksonDecoder implements Decoder {
    private static final String NO_CONTENT_MESSAGE = "No content to map due to end-of-input";
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
            response.status() == HttpURLConnection.HTTP_NO_CONTENT
                || response.body() == null
                || (response.body().length() != null && response.body().length() == 0)
            ) {
            return null;
        }

        try (final Reader reader = response.body().asReader()) {
            return this.mapper.readValue(reader, this.mapper.constructType(type));
        } catch (final JsonMappingException jme) {
            // The case where for whatever reason (most likely bad design) where the server returned OK and
            // trying to de-serialize the content had no content (e.g. the return status should have been no-content)
            if (response.status() == HttpURLConnection.HTTP_OK
                && jme.getMessage().startsWith(NO_CONTENT_MESSAGE)) {
                return null;
            }

            throw jme;
        } catch (final RuntimeJsonMappingException e) {
            if (e.getCause() != null && e.getCause() instanceof IOException) {
                throw IOException.class.cast(e.getCause());
            }
            throw e;
        }
    }
}
