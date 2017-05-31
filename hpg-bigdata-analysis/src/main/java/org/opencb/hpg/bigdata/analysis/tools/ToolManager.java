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

package org.opencb.hpg.bigdata.analysis.tools;

import org.apache.commons.lang3.StringUtils;
import org.opencb.hpg.bigdata.analysis.exceptions.AnalysisToolException;
import org.opencb.hpg.bigdata.analysis.tools.manifest.Execution;
import org.opencb.hpg.bigdata.analysis.tools.manifest.InputParam;
import org.opencb.hpg.bigdata.analysis.tools.manifest.Manifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by imedina on 19/05/17.
 */
public class ToolManager {

    private Path toolDirectory;
    private Map<String, Manifest> tools;
    private Logger logger;

    private final String MANIFEST_FILE = "manifest.json";

    public ToolManager(Path toolDirectory) throws AnalysisToolException {
        this.toolDirectory = toolDirectory;
        this.tools = new HashMap<>();
        this.logger = LoggerFactory.getLogger(ToolManager.class);

        checkToolDirectory();
    }

    public String createCommandLine(String tool, String executionName, Map<String, Object> paramsMap) throws AnalysisToolException {
        Manifest manifest = getManifest(tool);

        // Look for the execution
        Execution execution = null;
        for (Execution executionTmp : manifest.getExecutions()) {
            if (executionTmp.getId().equalsIgnoreCase(executionName)) {
                execution = executionTmp;
                break;
            }
        }
        if (execution == null) {
            throw new AnalysisToolException("Execution " + executionName + " not found in manifest");
        }
        Map<String, InputParam> manifestParams = execution.getParamsAsMap();

        validateParams(execution, paramsMap);

        // Pass the params to a sortedMap object sorted by the position defined in the manifest (if any)
        Comparator<String> comparator = new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                int i = getPosition(o1).compareTo(getPosition(o2));
                if (i != 0) {
                    return i;
                }
                return o1.compareTo(o2);
            }

            private Integer getPosition(String o) {
                InputParam inputParam = manifestParams.get(o);
                // If it is an output parameter with redirection, we put it at the end no matter the position configured
                return inputParam != null
                        ? (inputParam.isRedirection() ? Integer.MAX_VALUE : inputParam.getPosition())
                        : 0;
            }
        };
        SortedMap<String, Object> sortedMap = new TreeMap<>(comparator);
        sortedMap.putAll(paramsMap);

        StringBuilder commandLine = new StringBuilder(toolDirectory.resolve(tool).toString()).append("/")
                .append(execution.getExecutable()).append(" ");
        for (Map.Entry<String, Object> objectEntry : sortedMap.entrySet()) {
            InputParam inputParam = manifestParams.get(objectEntry.getKey());
            if (inputParam != null) {
                if (!inputParam.isHidden() && !inputParam.isRedirection()) {
                    // Parameter key should appear in the command line
                    String key = inputParam.getName();
                    if (!key.startsWith("-") && execution.isPosix()) {
                        key = (key.length() > 1 ? "--" : "-") + key;
                    }

                    commandLine.append(key).append(manifest.getSeparator());
                }

                // Value
                if (inputParam.getDataType() != InputParam.Type.BOOLEAN) {
                    if (inputParam.isRedirection()) {
                        commandLine.append("> ").append(objectEntry.getValue().toString());
                    } else {
                        commandLine.append(objectEntry.getValue().toString()).append(" ");
                    }
                } else if (inputParam.getArity() > 0) {
                    commandLine.append(objectEntry.getValue()).append(" ");
                }
            } else {
                // Parameter objectEntry.getKey() does not exist in manifest
                String key = objectEntry.getKey();
                if (!key.startsWith("-") && execution.isPosix()) {
                    key = (key.length() > 1 ? "--" : "-") + key;
                }

                // Key
                commandLine.append(key).append(manifest.getSeparator());
                // Value
                commandLine.append(objectEntry.getValue().toString()).append(" ");
            }
        }

        return commandLine.toString();
    }

    public Manifest getManifest(String tool) throws AnalysisToolException {
        if (StringUtils.isEmpty(tool)) {
            throw new AnalysisToolException("Missing tool name information");
        }

        Manifest manifest = this.tools.get(tool);
        if (manifest != null) {
            return manifest;
        }

        Path toolPath = this.toolDirectory.resolve(tool + "/" + MANIFEST_FILE);
        if (!toolPath.toFile().exists()) {
            throw new AnalysisToolException("Missing manifest file in " + toolPath);
        }

        try {
            manifest = Manifest.load(new FileInputStream(toolPath.toFile()));
        } catch (IOException e) {
            logger.error("Error reading manifest file: {}", e.getMessage(), e);
            throw new AnalysisToolException(e.getMessage(), e.getCause());
        }

        this.tools.put(tool, manifest);

        return manifest;
    }

    public void runCommandLine(String commandLine, Path outdir) throws AnalysisToolException {
        new Executor().execute(commandLine, outdir);
    }

    private void checkToolDirectory() throws AnalysisToolException {
        if (toolDirectory == null || !toolDirectory.toFile().isDirectory()) {
            throw new AnalysisToolException("Path to tool directory not found");
        }

        try {
            Files.list(toolDirectory)
                    .forEach(path -> {
                        if (path.toFile().isFile()) {
                            throw new RuntimeException("Found unexpected file in tool directory. Expecting only folders with tools");
                        }
                        if (!path.toFile().canRead()) {
                            throw new RuntimeException("Cannot read folder " + path);
                        }
                        if (!path.resolve(MANIFEST_FILE).toFile().exists()) {
                            throw new RuntimeException("Manifest file not found under tool folder " + path);
                        }
                    });
        } catch (IOException e) {
            logger.error("{}", e.getMessage(), e);
            throw new AnalysisToolException(e.getMessage(), e.getCause());
        }
    }

    private void validateParams(Execution execution, Map<String, Object> paramsMap) throws AnalysisToolException {
        Map<String, InputParam> manifestParams = execution.getParamsAsMap();

        // We fetch which are the required parameters and we will be taking them out as we check the user paramsMap.
        Set<String> requiredParams = new HashSet<>();
        for (InputParam inputParam : execution.getParams()) {
            if (inputParam.isRequired()) {
                requiredParams.add(inputParam.getName());
            }
        }

        for (Map.Entry<String, Object> entry : paramsMap.entrySet()) {
            InputParam inputParam = manifestParams.get(entry.getKey());
            if (inputParam != null) {
                if (inputParam.isRequired()) {
                    requiredParams.remove(entry.getKey());
                }
                validateDataType(entry, inputParam);
            } else {
                logger.info("Unknown parameter: {}", entry.getKey());
            }
        }

        if (requiredParams.size() > 0) {
            throw new AnalysisToolException("Some mandatory parameters not found: " + StringUtils.join(requiredParams, ","));
        }
    }

    private void validateDataType(Map.Entry<String, Object> entry, InputParam inputParam) throws AnalysisToolException {
        switch (inputParam.getDataType()) {
            case STRING:
            case BOOLEAN:
                break;
            case NUMERIC:
                if (!StringUtils.isNumeric((CharSequence) entry.getValue())) {
                    throw new AnalysisToolException("Unexpected value for parameter " + entry.getKey() + ". Expecting a NUMERIC value");
                }
                break;
            case FILE:
                Path path = Paths.get(String.valueOf(entry.getValue()));
                if (!inputParam.isOutput()) {
                    // Is input
                    if (!path.toFile().exists()) {
                        throw new AnalysisToolException("Input file " + path.toString() + " not found");
                    }
                    if (!path.toFile().canRead()) {
                        throw new AnalysisToolException("Cannot read input file " + path.toString());
                    }
                    if (!path.toFile().isFile()) {
                        throw new AnalysisToolException("Input " + path.toString() + " is not a file");
                    }
                } else {
                    // Is output
                    if (path.toFile().exists()) {
                        // It will be overwritten
                        if (!path.toFile().isFile()) {
                            throw new AnalysisToolException("Output " + path.toString() + " is not a file");
                        }
                        if (!path.toFile().canWrite()) {
                            throw new AnalysisToolException("Cannot write over file " + path.toString());
                        }
                    } else {
                        File dirPath = path.toFile().getParentFile();
                        if (dirPath == null) {
                            throw new AnalysisToolException("Writing in the / folder is not allowed");
                        }
                        if (!dirPath.exists()) {
                            throw new AnalysisToolException("The folder " + dirPath.toString()
                                    + " to write the output file does not exist");
                        }
                        if (!dirPath.canWrite()) {
                            throw new AnalysisToolException("Cannot create output file in " + dirPath.toString()
                                    + ". No write permissions.");
                        }
                    }
                }
                break;
            case FOLDER:
                path = Paths.get(String.valueOf(entry.getValue()));
                if (!inputParam.isOutput()) {
                    // Is input
                    if (!path.toFile().exists()) {
                        throw new AnalysisToolException("Input folder " + path.toString() + " not found");
                    }
                    if (!path.toFile().canRead()) {
                        throw new AnalysisToolException("Cannot read input folder " + path.toString());
                    }
                    if (!path.toFile().isDirectory()) {
                        throw new AnalysisToolException("Input " + path.toString() + " is not a directory");
                    }
                } else {
                    // Is output
                    if (!path.toFile().exists()) {
                        throw new AnalysisToolException("Output folder " + path.toString() + " not found");
                    }
                    if (!path.toFile().canWrite()) {
                        throw new AnalysisToolException("Cannot write on folder folder " + path.toString());
                    }
                    if (!path.toFile().isDirectory()) {
                        throw new AnalysisToolException("Output " + path.toString() + " is not a directory");
                    }
                }
                break;
            default:
                // TODO: This should be checked when loading the manifest file for the first time
                throw new AnalysisToolException("Unknown type of parameter " + inputParam.getName());
        }
    }

}
