/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.hpg.bigdata.core.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opencb.biodata.models.core.Region;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 08/10/15.
 */
public class SimulatorConfiguration {

    private String logLevel;
    private String logFile;

    private List<Region> regions;
    private Map<String, Float> genotypeProbabilities;

    protected static Logger logger = LoggerFactory.getLogger(SimulatorConfiguration.class);

    public SimulatorConfiguration() {
    }

    /*
     * This method attempts to find and load the configuration from installation directory,
     * if not exists then loads JAR storage-configuration.yml.
     */
    @Deprecated
    public static SimulatorConfiguration load() throws IOException {
        String appHome = System.getProperty("app.home", System.getenv("BIONETDB_HOME"));
        Path path = Paths.get(appHome + "/simulator-conf.yml");
        if (appHome != null && Files.exists(path)) {
            logger.debug("Loading configuration from '{}'", appHome + "/simulator-conf.yml");
            return SimulatorConfiguration
                    .load(new FileInputStream(new File(appHome + "/simulator-conf.yml")));
        } else {
            logger.debug("Loading configuration from '{}'",
                    SimulatorConfiguration.class.getClassLoader()
                            .getResourceAsStream("simulator-conf.yml")
                            .toString());
            return SimulatorConfiguration
                    .load(SimulatorConfiguration.class.getClassLoader().getResourceAsStream("simulator-conf.yml"));
        }
    }

    public static SimulatorConfiguration load(InputStream configurationInputStream) throws IOException {
        return load(configurationInputStream, "yaml");
    }

    public static SimulatorConfiguration load(InputStream configurationInputStream, String format) throws IOException {
        SimulatorConfiguration simulatorConfiguration;
        ObjectMapper objectMapper;
        switch (format) {
            case "json":
                objectMapper = new ObjectMapper();
                simulatorConfiguration = objectMapper.readValue(configurationInputStream, SimulatorConfiguration.class);
                break;
            case "yml":
            case "yaml":
            default:
                objectMapper = new ObjectMapper(new YAMLFactory());
                simulatorConfiguration = objectMapper.readValue(configurationInputStream, SimulatorConfiguration.class);
                break;
        }

        return simulatorConfiguration;
    }

    public void serialize(OutputStream configurationOututStream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper(new YAMLFactory());
        jsonMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOututStream, this);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SimulatorConfiguration{");
        sb.append("logLevel='").append(logLevel).append('\'');
        sb.append(", logFile='").append(logFile).append('\'');
        sb.append(", regions=").append(regions);
        sb.append(", genotypeProbabilities=").append(genotypeProbabilities);
        sb.append('}');
        return sb.toString();
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public String getLogFile() {
        return logFile;
    }

    public void setLogFile(String logFile) {
        this.logFile = logFile;
    }

    public List<Region> getRegions() {
        return regions;
    }

    public void setRegions(List<Region> regions) {
        this.regions = regions;
    }

    public Map<String, Float> getGenotypeProbabilities() {
        return genotypeProbabilities;
    }

    public void setGenotypeProbabilities(Map<String, Float> genotypeProbabilities) {
        this.genotypeProbabilities = genotypeProbabilities;
    }
}
