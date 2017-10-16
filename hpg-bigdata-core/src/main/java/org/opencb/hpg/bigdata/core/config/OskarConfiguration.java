package org.opencb.hpg.bigdata.core.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class OskarConfiguration {

    private String logLevel;
    private String logFile;

    private String toolFolder;

    private CellBaseConfiguration cellbase;


    public OskarConfiguration() {
        this.cellbase = new CellBaseConfiguration();
    }

    public static OskarConfiguration load(InputStream configurationInputStream) throws IOException {
        return load(configurationInputStream, "yaml");
    }

    public static OskarConfiguration load(InputStream configurationInputStream, String format) throws IOException {
        OskarConfiguration configuration;
        ObjectMapper objectMapper;
        switch (format) {
            case "yml":
            case "yaml":
            default:
                objectMapper = new ObjectMapper(new YAMLFactory());
                configuration = objectMapper.readValue(configurationInputStream, OskarConfiguration.class);
                break;
        }

        return configuration;
    }

    public void serialize(OutputStream configurationOutputStream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper(new YAMLFactory());
        jsonMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOutputStream, this);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OskarConfiguration{");
        sb.append("logLevel='").append(logLevel).append('\'');
        sb.append(", logFile='").append(logFile).append('\'');
        sb.append(", toolFolder='").append(toolFolder).append('\'');
        sb.append(", cellbase=").append(cellbase);
        sb.append('}');
        return sb.toString();
    }

    public String getLogLevel() {
        return logLevel;
    }

    public OskarConfiguration setLogLevel(String logLevel) {
        this.logLevel = logLevel;
        return this;
    }

    public String getLogFile() {
        return logFile;
    }

    public OskarConfiguration setLogFile(String logFile) {
        this.logFile = logFile;
        return this;
    }

    public String getToolFolder() {
        return toolFolder;
    }

    public OskarConfiguration setToolFolder(String toolFolder) {
        this.toolFolder = toolFolder;
        return this;
    }

    public CellBaseConfiguration getCellbase() {
        return cellbase;
    }

    public OskarConfiguration setCellbase(CellBaseConfiguration cellbase) {
        this.cellbase = cellbase;
        return this;
    }
}
