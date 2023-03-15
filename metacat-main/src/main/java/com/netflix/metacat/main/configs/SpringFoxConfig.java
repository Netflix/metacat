package com.netflix.metacat.main.configs;

import org.springframework.boot.actuate.autoconfigure.endpoint.web.CorsEndpointProperties;
import org.springframework.boot.actuate.autoconfigure.endpoint.web.WebEndpointProperties;
import org.springframework.boot.actuate.autoconfigure.web.server.ManagementPortType;
import org.springframework.boot.actuate.endpoint.ExposableEndpoint;
import org.springframework.boot.actuate.endpoint.web.EndpointLinksResolver;
import org.springframework.boot.actuate.endpoint.web.EndpointMapping;
import org.springframework.boot.actuate.endpoint.web.EndpointMediaTypes;
import org.springframework.boot.actuate.endpoint.web.ExposableWebEndpoint;
import org.springframework.boot.actuate.endpoint.web.WebEndpointsSupplier;
import org.springframework.boot.actuate.endpoint.web.annotation.ControllerEndpointsSupplier;
import org.springframework.boot.actuate.endpoint.web.annotation.ServletEndpointsSupplier;
import org.springframework.boot.actuate.endpoint.web.servlet.WebMvcEndpointHandlerMapping;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Needed to get SpringFox working with SBN 2.6+.
 */
@Configuration
public class SpringFoxConfig {
    /**
     * Needed this bean initialization to have springfox work with SBN 2.6+.
     *
     * @param webEndpointsSupplier  web endpoint supplier
     * @param servletEndpointsSupplier servlet endpoint supplier
     * @param controllerEndpointsSupplier controller endpoint supplier
     * @param endpointMediaTypes media types
     * @param corsProperties CORS properties
     * @param webEndpointProperties web endpoint properties
     * @param environment application environment
     * @return WebMvcEndpointHandlerMapping
     */
    @Bean
    public WebMvcEndpointHandlerMapping webEndpointServletHandlerMapping(
        final WebEndpointsSupplier webEndpointsSupplier,
        final ServletEndpointsSupplier servletEndpointsSupplier,
        final ControllerEndpointsSupplier controllerEndpointsSupplier,
        final EndpointMediaTypes endpointMediaTypes,
        final CorsEndpointProperties corsProperties,
        final WebEndpointProperties webEndpointProperties,
        final Environment environment) {
        final List<ExposableEndpoint<?>> allEndpoints = new ArrayList<>();
        final Collection<ExposableWebEndpoint> webEndpoints = webEndpointsSupplier.getEndpoints();
        allEndpoints.addAll(webEndpoints);
        allEndpoints.addAll(servletEndpointsSupplier.getEndpoints());
        allEndpoints.addAll(controllerEndpointsSupplier.getEndpoints());

        final String basePath = webEndpointProperties.getBasePath();
        final EndpointMapping endpointMapping = new EndpointMapping(basePath);

        final boolean shouldRegisterLinksMapping = this.shouldRegisterLinksMapping(
            webEndpointProperties, environment, basePath);

        return new WebMvcEndpointHandlerMapping(endpointMapping, webEndpoints, endpointMediaTypes,
            corsProperties.toCorsConfiguration(), new EndpointLinksResolver(allEndpoints, basePath),
            shouldRegisterLinksMapping, null);
    }

    private boolean shouldRegisterLinksMapping(final WebEndpointProperties webEndpointProperties,
                                               final Environment environment,
                                               final String basePath) {
        return webEndpointProperties.getDiscovery().isEnabled()
                   && (StringUtils.hasText(basePath)
                           || ManagementPortType.get(environment).equals(ManagementPortType.DIFFERENT));
    }
}
