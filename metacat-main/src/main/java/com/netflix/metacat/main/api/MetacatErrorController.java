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

import org.springframework.boot.autoconfigure.web.AbstractErrorController;
import org.springframework.boot.autoconfigure.web.ErrorAttributes;
import org.springframework.boot.autoconfigure.web.ErrorProperties;
import org.springframework.boot.autoconfigure.web.ErrorViewResolver;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.util.Collections;
import java.util.Map;

/**
 * Error controller.
 */
@Controller
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
        final Map<String, Object> body = getErrorAttributes(request, isIncludeStackTrace(request));
        final HttpStatus status = getStatus(request);
        return new ResponseEntity<>(body, status);
    }

    /**
     * Determine if the stacktrace attribute should be included.
     * @param request the source request
     * @return if the stacktrace attribute should be included
     */
    private boolean isIncludeStackTrace(final HttpServletRequest request) {
        final ErrorProperties.IncludeStacktrace include = this.errorProperties.getIncludeStacktrace();
        return include == ErrorProperties.IncludeStacktrace.ALWAYS
            || include == ErrorProperties.IncludeStacktrace.ON_TRACE_PARAM && getTraceParameter(request);
    }

    @Override
    public String getErrorPath() {
        return this.errorProperties.getPath();
    }
}
