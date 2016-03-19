package com.netflix.metacat.common.monitoring;

/**
 * Created by amajumdar on 11/4/14.
 */
public enum LogConstants {
    /*
    General logging constants
     */
    GlobalPrefix("dse"),
    AppPrefix(GlobalPrefix + ".metacat"),
    /*
    Counters
     */
    CounterCreateCatalog(AppPrefix+".countCreateCatalog"),
    CounterCreateTable(AppPrefix+".countCreateTable"),
    CounterCreateDatabase(AppPrefix + ".countCreateDatabase"),
    CounterCreateMView(AppPrefix + ".countCreateMView"),
    CounterDeleteDatabase(AppPrefix + ".countDeleteDatabase"),
    CounterDeleteTablePartitions(AppPrefix + ".countDeleteTablePartitions"),
    CounterDeleteMViewPartitions(AppPrefix + ".countDeleteMViewPartitions"),
    CounterDeleteTable(AppPrefix + ".countDropTable"),
    CounterDeleteMView(AppPrefix + ".countDeleteMView"),
    CounterGetCatalog(AppPrefix+".countGetMetadata"),
    CounterGetCatalogNames(AppPrefix+".countGetCatalogNames"),
    CounterGetDatabase(AppPrefix+".countGetDatabase"),
    CounterGetMViewPartitions(AppPrefix+".countGetMViewPartitions"),
    CounterGetTablePartitions(AppPrefix+".countGetTablePartitions"),
    CounterGetTable(AppPrefix+".countGetTable"),
    CounterGetMView(AppPrefix+".countGetMView"),
    CounterGetCatalogMViews(AppPrefix+".countGetCatalogMViews"),
    CounterGetTableMViews(AppPrefix+".countGetTableMViews"),
    CounterRenameTable(AppPrefix+".countRenameTable"),
    CounterUpdateCatalog(AppPrefix+".countUpdateCatalog"),
    CounterUpdateTable(AppPrefix+".countUpdateTable"),
    CounterSaveTablePartitions(AppPrefix+".countSaveTablePartitions"),
    CounterSaveMViewPartitions(AppPrefix+".countSaveMViewPartitions"),
    CounterCreateCatalogFailure(AppPrefix+".countCreateCatalogFailure"),
    CounterCreateTableFailure(AppPrefix+".countCreateTableFailure"),
    CounterCreateDatabaseFailure(AppPrefix + ".countCreateDatabaseFailure"),
    CounterCreateMViewFailure(AppPrefix + ".countCreateMViewFailure"),
    CounterDeleteDatabaseFailure(AppPrefix + ".countDeleteDatabaseFailure"),
    CounterDeleteTablePartitionsFailure(AppPrefix + ".countDeleteTablePartitionsFailure"),
    CounterDeleteMViewPartitionsFailure(AppPrefix + ".countDeleteMViewPartitionsFailure"),
    CounterDeleteTableFailure(AppPrefix + ".countDropTableFailure"),
    CounterDeleteMViewFailure(AppPrefix + ".countDeleteMViewFailure"),
    CounterGetCatalogFailure(AppPrefix+".countGetMetadataFailure"),
    CounterGetCatalogNamesFailure(AppPrefix+".countGetCatalogNamesFailure"),
    CounterGetDatabaseFailure(AppPrefix+".countGetDatabaseFailure"),
    CounterGetMViewPartitionsFailure(AppPrefix+".countGetMViewPartitionsFailure"),
    CounterGetTablePartitionsFailure(AppPrefix+".countGetTablePartitionsFailure"),
    CounterGetTableFailure(AppPrefix+".countGetTableFailure"),
    CounterGetMViewFailure(AppPrefix+".countGetMViewFailure"),
    CounterGetCatalogMViewsFailure(AppPrefix+".countGetCatalogMViewsFailure"),
    CounterGetTableMViewsFailure(AppPrefix+".countGetTableMViewsFailure"),
    CounterRenameTableFailure(AppPrefix+".countRenameTableFailure"),
    CounterUpdateCatalogFailure(AppPrefix+".countUpdateCatalogFailure"),
    CounterUpdateTableFailure(AppPrefix+".countUpdateTableFailure"),
    CounterSaveTablePartitionsFailure(AppPrefix+".countSaveTablePartitionsFailure"),
    CounterSaveMViewPartitionsFailure(AppPrefix+".countSaveMViewPartitionsFailure"),
    /*
    Tracers
     */
    TracerCreateCatalog(AppPrefix+".traceCreateCatalog"),
    TracerCreateTable(AppPrefix+".traceCreateTable"),
    TracerCreateDatabase(AppPrefix + ".traceCreateDatabase"),
    TracerCreateMView(AppPrefix + ".traceCreateMView"),
    TracerDeleteDatabase(AppPrefix + ".traceDeleteDatabase"),
    TracerDeleteTablePartitions(AppPrefix + ".traceDeleteTablePartitions"),
    TracerDeleteMViewPartitions(AppPrefix + ".traceDeleteMViewPartitions"),
    TracerDeleteTable(AppPrefix + ".traceDropTable"),
    TracerDeleteMView(AppPrefix + ".traceDeleteMView"),
    TracerGetCatalog(AppPrefix+".traceGetMetadata"),
    TracerGetCatalogNames(AppPrefix+".traceGetCatalogNames"),
    TracerGetDatabase(AppPrefix+".traceGetDatabase"),
    TracerGetMViewPartitions(AppPrefix+".traceGetMViewPartitions"),
    TracerGetTablePartitions(AppPrefix+".traceGetTablePartitions"),
    TracerGetTable(AppPrefix+".traceGetTable"),
    TracerGetMView(AppPrefix+".traceGetMView"),
    TracerGetCatalogMViews(AppPrefix+".traceGetCatalogMViews"),
    TracerGetTableMViews(AppPrefix+".traceGetTableMViews"),
    TracerRenameTable(AppPrefix+".traceRenameTable"),
    TracerUpdateCatalog(AppPrefix+".traceUpdateCatalog"),
    TracerUpdateTable(AppPrefix+".traceUpdateTable"),
    TracerSaveTablePartitions(AppPrefix+".traceSaveTablePartitions"),
    TracerSaveMViewPartitions(AppPrefix+".traceSaveMViewPartitions"),
    /*
    Gauges
     */
    GaugeAddPartitions(AppPrefix+".gaugeAddPartitions"),
    GaugeDeletePartitions(AppPrefix+".gaugeDeletePartitions"),
    GaugeGetPartitionsCount(AppPrefix+".gaugeGetPartitionsCount");

    private final String constant;

    LogConstants(String constant) {
        this.constant = constant;
    }

    @Override
    public String toString() {
        return constant;
    }
}
