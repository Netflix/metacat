package com.netflix.metacat.common.server.events;

import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.TableDto;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * Event fired after an Iceberg table has been updated.
 */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class MetacatUpdateIcebergTablePostEvent extends MetacatEvent {
    private final TableDto oldTable;
    private final TableDto requestTable;

    /**
     * Constructor.
     *
     * @param name           The name of the Table.
     * @param requestContext The request context.
     * @param source         The source of this event.
     * @param oldTable       The old Table instance.
     * @param requestTable   The TableDto instance sent in the request.
     */
    public MetacatUpdateIcebergTablePostEvent(
        final QualifiedName name,
        final MetacatRequestContext requestContext,
        final Object source,
        @NonNull final TableDto oldTable,
        @NonNull final TableDto requestTable
    ) {
        super(name, requestContext, source);
        this.oldTable = oldTable;
        this.requestTable = requestTable;
    }
}
