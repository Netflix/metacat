package com.netflix.metacat.main.services;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.server.events.AsyncListener;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.events.MetacatUpdateIcebergTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePostEvent;
import com.netflix.metacat.common.server.monitoring.Metrics;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Handler for Iceberg table specific events.
 */
@Slf4j
@Component
@AsyncListener
public class IcebergTableEventHandler {
    private final TableService tableService;
    private final MetacatEventBus eventBus;
    private final Registry registry;

    /**
     * Constructor.
     *
     * @param tableService The table service.
     * @param eventBus     The metacat event bus.
     * @param registry     The registry.
     */
    @Autowired
    public IcebergTableEventHandler(
        final TableService tableService,
        final MetacatEventBus eventBus,
        final Registry registry
    ) {
        this.tableService = tableService;
        this.eventBus = eventBus;
        this.registry = registry;
    }

    /**
     * The update table event handler.
     *
     * @param event The event.
     */
    @EventListener
    public void metacatUpdateTableEventHandler(final MetacatUpdateIcebergTablePostEvent event) {
        final QualifiedName name = event.getName();
        final TableDto tableDto = event.getRequestTable();
        TableDto updatedDto = tableDto;
        try {
            updatedDto = tableService.get(name,
                GetTableServiceParameters.builder()
                    .disableOnReadMetadataIntercetor(false)
                    .includeInfo(true)
                    .includeDataMetadata(true)
                    .includeDefinitionMetadata(true)
                    .build()).orElse(tableDto);
        } catch (Exception ex) {
            handleException(name, "getTable", ex);
        }

        try {
            eventBus.post(new MetacatUpdateTablePostEvent(event.getName(), event.getRequestContext(),
                this, event.getOldTable(),
                updatedDto, updatedDto != tableDto));
        } catch (Exception ex) {
            handleException(name, "postEvent", ex);
        }
    }

    private void handleException(final QualifiedName name,
                                 final String request,
                                 final Exception ex) {
        log.warn("Failed {} for table {}. Error: {}", request, name, ex.getMessage());
        registry.counter(registry.createId(
            Metrics.CounterTableUpdateIgnoredException.getMetricName()).withTags(name.parts())
            .withTag("request", request)).increment();
    }
}
