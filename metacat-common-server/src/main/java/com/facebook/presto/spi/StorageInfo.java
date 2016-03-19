package com.facebook.presto.spi;

import java.util.Map;

/**
 * Created by amajumdar on 3/3/15.
 */
public class StorageInfo {
    /* Location of the data */
    private String uri;
    /* Input format of the file */
    private String inputFormat;
    /* Output format of the file */
    private String outputFormat;
    /* Serialization library */
    private String serializationLib;
    /* Serialization parameters */
    private Map<String, String> parameters;
    private Map<String, String> serdeInfoParameters;

    public StorageInfo() {
    }

    public StorageInfo(String uri, String inputFormat, String outputFormat, String serializationLib,
            Map<String, String> parameters, Map<String, String> serdeInfoParameters) {
        this.uri = uri;
        this.inputFormat = inputFormat;
        this.outputFormat = outputFormat;
        this.serializationLib = serializationLib;
        this.parameters = parameters;
        this.serdeInfoParameters = serdeInfoParameters;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public void setInputFormat(String inputFormat) {
        this.inputFormat = inputFormat;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public void setOutputFormat(String outputFormat) {
        this.outputFormat = outputFormat;
    }

    public String getSerializationLib() {
        return serializationLib;
    }

    public void setSerializationLib(String serializationLib) {
        this.serializationLib = serializationLib;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public Map<String, String> getSerdeInfoParameters() {
        return serdeInfoParameters;
    }

    public void setSerdeInfoParameters(Map<String, String> serdeInfoParameters) {
        this.serdeInfoParameters = serdeInfoParameters;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }
}
