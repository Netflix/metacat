/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.metacat.main.api;

import com.netflix.metacat.common.exception.MetacatAlreadyExistsException;
import com.netflix.metacat.common.exception.MetacatBadRequestException;
import com.netflix.metacat.common.exception.MetacatException;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.exception.MetacatNotSupportedException;
import com.netflix.metacat.common.exception.MetacatUserMetadataException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Exception mapper for Genie Exceptions.
 *
 * @author tgianos
 * @since 1.1.0
 */
@Slf4j
@ControllerAdvice
public class ExceptionMapper {

    /**
     * Handle Genie Exceptions.
     *
     * @param response The HTTP response
     * @param e        The exception to handle
     * @throws IOException on error in sending error
     */
    @ExceptionHandler(MetacatException.class)
    public void handleGenieException(
        final HttpServletResponse response,
        final MetacatException e
    ) throws IOException {
        final int status;
        if (e instanceof MetacatAlreadyExistsException) {
            status = HttpStatus.CONFLICT.value();
        } else if (e instanceof MetacatBadRequestException) {
            status = HttpStatus.BAD_REQUEST.value();
        } else if (e instanceof MetacatNotFoundException) {
            status = HttpStatus.NOT_FOUND.value();
        } else if (e instanceof MetacatNotSupportedException) {
            status = HttpStatus.NOT_IMPLEMENTED.value();
        } else if (e instanceof MetacatUserMetadataException) {
            // TODO: This makes no sense
            status = HttpStatus.SEE_OTHER.value();
        } else {
            status = HttpStatus.INTERNAL_SERVER_ERROR.value();
        }
        log.error(e.getLocalizedMessage(), e);
        response.sendError(status, e.getLocalizedMessage());
    }
}
