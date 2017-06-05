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
package com.netflix.metacat.main.configs;

import com.netflix.metacat.main.api.ApiFilter;
import org.springframework.boot.autoconfigure.web.DefaultErrorAttributes;
import org.springframework.boot.autoconfigure.web.ErrorAttributes;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.context.request.RequestAttributes;

import java.util.Map;

/**
 * Spring configuration for the API tier.
 *
 * @author tgianos
 * @since 1.1.0
 */
@Configuration
public class ApiConfig {

    /**
     * The rest filter registration bean.
     *
     * @return The rest filter
     */
    @Bean
    public FilterRegistrationBean metacatApiFilter() {
        final FilterRegistrationBean registrationBean = new FilterRegistrationBean();
        final ApiFilter filter = new ApiFilter();
        registrationBean.setFilter(filter);
        registrationBean.addUrlPatterns("/mds/*");
        return registrationBean;
    }

    /**
     * Override the default error attributes for backwards compatibility with older clients.
     *
     * @return New set of error attributes with 'message' copied into 'error'
     */
    @Bean
    public ErrorAttributes errorAttributes() {
        return new DefaultErrorAttributes() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Map<String, Object> getErrorAttributes(
                final RequestAttributes requestAttributes,
                final boolean includeStackTrace
            ) {
                final Map<String, Object> errorAttributes
                    = super.getErrorAttributes(requestAttributes, includeStackTrace);
                errorAttributes.put("error", errorAttributes.get("message"));
                return errorAttributes;
            }
        };
    }
}
