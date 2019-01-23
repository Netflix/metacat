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
import com.netflix.metacat.main.api.MetacatErrorController;
import org.springframework.boot.web.servlet.error.DefaultErrorAttributes;
import org.springframework.boot.web.servlet.error.ErrorAttributes;
import org.springframework.boot.autoconfigure.web.ErrorProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import java.util.Map;

/**
 * Spring configuration for the API tier.
 *
 * @author tgianos
 * @since 1.1.0
 */
@Configuration
public class ApiConfig extends WebMvcConfigurerAdapter {
    /**
     * {@inheritDoc}
     * <p>
     * Turn off {@literal .} Turn off suffix-based content negotiation. The table name may have extension, e.g. knp,
     * , which is a type and will be rejected by spring
     *
     * @see <a href="https://stackoverflow.com/questions/30793717">Stack Overflow Issue</a>
     */
    @Override
    public void configureContentNegotiation(final ContentNegotiationConfigurer configurer) {
        configurer.favorPathExtension(false);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Turn off {@literal .} recognition in paths. Needed due to table's name potentially having '.' as character.
     *
     * @see <a href="https://docs.spring.io/spring/docs/current/spring-framework-reference/html/mvc.html">SpringDoc</a>
     */
    @Override
    public void configurePathMatch(final PathMatchConfigurer configurer) {
        configurer.setUseSuffixPatternMatch(false);
    }

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
                final WebRequest webRequest,
                final boolean includeStackTrace
            ) {
                final Map<String, Object> errorAttributes
                    = super.getErrorAttributes(webRequest, includeStackTrace);
                errorAttributes.put("error", errorAttributes.get("message"));
                return errorAttributes;
            }
        };
    }

    /**
     * Returns the default error properties.
     * @return default error properties.
     */
    @Bean
    public ErrorProperties errorProperties() {
        return new ErrorProperties();
    }

    /**
     * Returns the error controller.
     * @param errorAttributes error attributes
     * @param errorProperties error properties
     * @return error controller
     */
    @Bean
    public MetacatErrorController metacatErrorController(final ErrorAttributes errorAttributes,
                                                         final ErrorProperties errorProperties) {
        return new MetacatErrorController(errorAttributes, errorProperties);
    }
}
