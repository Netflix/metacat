package com.netflix.metacat.client.module;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.exception.MetacatAlreadyExistsException;
import com.netflix.metacat.common.exception.MetacatBadRequestException;
import com.netflix.metacat.common.exception.MetacatException;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.exception.MetacatNotSupportedException;
import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.json.MetacatJsonException;
import com.netflix.metacat.common.json.MetacatJsonLocator;
import feign.Response;
import feign.RetryableException;
import feign.Util;

import java.io.IOException;

import static javax.ws.rs.core.Response.Status;

/**
 * Module that provides a error decoder, used to parse errors
 */
public class MetacatErrorDecoder extends feign.codec.ErrorDecoder.Default {
    private static final MetacatJson metacatJson = MetacatJsonLocator.INSTANCE;
        @Override
        public Exception decode(String methodKey, Response response){
            try {
                Status status = Status.fromStatusCode(response.status());
                String message = "";
                if (response.body() != null){
                    message = Util.toString(response.body().asReader());
                    try {
                        ObjectNode body = metacatJson.parseJsonObject(message);
                        message = body.path("error").asText();
                    } catch (MetacatJsonException ignored) {}
                }
                switch (status) {
                    case UNSUPPORTED_MEDIA_TYPE:
                        return new MetacatNotSupportedException(message);
                    case BAD_REQUEST:
                        return new MetacatBadRequestException(message);
                    case NOT_FOUND:
                        return new MetacatNotFoundException(message);
                    case CONFLICT:
                        return new MetacatAlreadyExistsException(message);
                    case INTERNAL_SERVER_ERROR:
                    case SERVICE_UNAVAILABLE:
                        return new RetryableException(message, null);
                    default:
                        return new MetacatException(message, Status.INTERNAL_SERVER_ERROR, null);
                }
            } catch (IOException e) {
                return super.decode(methodKey, response);
            }
        }
}
