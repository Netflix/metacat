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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.exception.MetacatAlreadyExistsException;
import com.netflix.metacat.common.exception.MetacatBadRequestException;
import com.netflix.metacat.common.exception.MetacatException;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.exception.MetacatNotSupportedException;
import com.netflix.metacat.common.exception.MetacatPreconditionFailedException;
import com.netflix.metacat.common.exception.MetacatTooManyRequestsException;
import com.netflix.metacat.common.exception.MetacatUnAuthorizedException;
import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.json.MetacatJsonException;
import feign.Response;
import feign.RetryableException;
import feign.Util;
import lombok.AllArgsConstructor;

import java.io.IOException;

/**
 * Module that provides a error decoder, used to parse errors.
 *
 * @author amajumdar
 */
@AllArgsConstructor
public class MetacatErrorDecoder extends feign.codec.ErrorDecoder.Default {
    private final MetacatJson metacatJson;

    /**
     * {@inheritDoc}
     */
    @Override
    public Exception decode(final String methodKey, final Response response) {
        try {
            String message = "";
            if (response.body() != null) {
                message = Util.toString(response.body().asReader());
                try {
                    final ObjectNode body = metacatJson.parseJsonObject(message);
                    message = body.path("message").asText("No error message supplied.");
                } catch (final MetacatJsonException ignored) {
                }
            }
            switch (response.status()) {
                case 501: //NOT IMPLEMENTED
                case 415: //UNSUPPORTED_MEDIA_TYPE
                    return new MetacatNotSupportedException(message);
                case 400: //BAD_REQUEST
                    return new MetacatBadRequestException(message);
                case 403: //Forbidden
                    return new MetacatUnAuthorizedException(message);
                case 404: //NOT_FOUND
                    return new MetacatNotFoundException(message);
                case 409: //CONFLICT
                    return new MetacatAlreadyExistsException(message);
                case 412: // PRECONDITION_FAILED
                    return new MetacatPreconditionFailedException(message);
                case 429:
                    return new RetryableException(message, new MetacatTooManyRequestsException(message), null);
                case 500: //INTERNAL_SERVER_ERROR
                case 503: //SERVICE_UNAVAILABLE
                    return new RetryableException(message, new MetacatException(message), null);
                default:
                    return new MetacatException(message);
            }
        } catch (final IOException e) {
            return super.decode(methodKey, response);
        }
    }
}
