/*
 *
 * Copyright 2018 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */
package com.netflix.metacat.main.api;

import org.springframework.boot.autoconfigure.web.servlet.error.AbstractErrorController;
import org.springframework.boot.web.error.ErrorAttributeOptions;
import org.springframework.boot.web.servlet.error.ErrorAttributes;
import org.springframework.boot.autoconfigure.web.ErrorProperties;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorViewResolver;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import jakarta.servlet.http.HttpServletRequest;
import java.util.Collections;
import java.util.Map;

/**
 * Error controller.
 *
 * @author amajumdar
 * @since 1.2.0
 */
@RequestMapping("${server.error.path:${error.path:/error}}")
public class MetacatErrorController extends AbstractErrorController {
    private final ErrorProperties errorProperties;
    /**
     * Default constructor.
     * @param errorAttributes error attributes
     * @param errorProperties error properties
     */
    public MetacatErrorController(final ErrorAttributes errorAttributes, final ErrorProperties errorProperties) {
        super(errorAttributes, Collections.<ErrorViewResolver>emptyList());
        this.errorProperties = errorProperties;
    }

    /**
     * Mapping for error handling.
     * @param request http request
     * @return error response
     */
    @RequestMapping
    @ResponseBody
    public ResponseEntity<Map<String, Object>> error(final HttpServletRequest request) {
        final Map<String, Object> body = getErrorAttributes(request, getErrorAttributeOptions(request));
        final HttpStatus status = getStatus(request);
        return new ResponseEntity<>(body, status);
    }

    private ErrorAttributeOptions getErrorAttributeOptions(final HttpServletRequest request) {
        ErrorAttributeOptions options = ErrorAttributeOptions.defaults();
        if (includeStackTrace(request)) {
            options = options.including(ErrorAttributeOptions.Include.STACK_TRACE);
        }
        if (includeMessage(request)) {
            options = options.including(ErrorAttributeOptions.Include.MESSAGE);
        }
        return options;
    }

    @SuppressWarnings("deprecation")
    private boolean includeStackTrace(final HttpServletRequest request) {
        switch (this.errorProperties.getIncludeStacktrace()) {
            case ALWAYS:
                return true;
            case ON_PARAM:
                return getBooleanParameter(request, "trace");
            default:
                return false;
        }
    }

    private boolean includeMessage(final HttpServletRequest request) {
        switch (this.errorProperties.getIncludeMessage()) {
            case ALWAYS:
                return true;
            case ON_PARAM:
                return getBooleanParameter(request, "message");
            default:
                return false;
        }
    }
}
