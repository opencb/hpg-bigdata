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
}
