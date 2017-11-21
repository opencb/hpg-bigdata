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

package org.opencb.hpg.bigdata.app.cli.executors;

import com.beust.jcommander.JCommander;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.opencb.hpg.bigdata.app.cli.options.LocalCliOptionsParser;
import org.opencb.hpg.bigdata.core.config.OskarConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by imedina on 03/02/15.
 */
public abstract class CommandExecutor {

    protected String logLevel;
    protected boolean verbose;
    @Deprecated
    protected String configFile;

    protected OskarConfiguration configuration;

    protected String appHome;

    protected Logger logger;

    protected LocalCliOptionsParser.CommonCommandOptions options;

    public CommandExecutor(LocalCliOptionsParser.CommonCommandOptions options) {
        this.options = options;

        init(options.logLevel, options.verbose, options.conf);
    }

    @Deprecated
    public CommandExecutor(String logLevel) {
        this(logLevel, false, null);
    }

    @Deprecated
    public CommandExecutor(String logLevel, boolean verbose, String configFile) {
        this.logLevel = logLevel;
        this.verbose = verbose;
        this.configFile = configFile;

        /**
         * System property 'app.home' is set up by cellbase.sh. If by any reason this is null
         * then CELLBASE_HOME environment variable is used instead.
         */
        this.appHome = System.getProperty("app.home", "/opt/hpg-bigdata");

        if (logLevel == null || logLevel.isEmpty()) {
            logLevel = "info";
        }
        // We must call to this method
        setLogLevel(logLevel);
    }

    protected void init(String logLevel, boolean verbose, String configFile) {
        this.logLevel = logLevel;
        this.verbose = verbose;
        this.configFile = configFile;

        /**
         * System property 'app.home' is set up by cellbase.sh. If by any reason this is null
         * then CELLBASE_HOME environment variable is used instead.
         */
        this.appHome = System.getProperty("app.home", "/opt/hpg-bigdata");

        try {
            Path confPath;
            // We first read the CLi parameter, otherwise we assume the default configuration file
            if (StringUtils.isNotEmpty(configFile) && new File(configFile).exists()) {
                confPath = Paths.get(configFile);
            } else {
                confPath = Paths.get(this.appHome).resolve("configuration.yml");
            }

            // We load the config file from disk or from the JAR file
            if (Files.exists(confPath)) {
                System.out.println("Loading configuration from '" + confPath.toAbsolutePath() + "'");
                this.configuration = OskarConfiguration.load(new FileInputStream(confPath.toFile()));
            } else {
                System.out.println("Loading configuration from JAR file");
                this.configuration = OskarConfiguration
                        .load(OskarConfiguration.class.getClassLoader().getResourceAsStream("configuration.yml"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (logLevel == null || logLevel.isEmpty()) {
            logLevel = "info";
        }
        // We must call to this method
        setLogLevel(logLevel);
    }

    public abstract void execute() throws Exception;

    protected static String getParsedSubCommand(JCommander jCommander) {
        String parsedCommand = jCommander.getParsedCommand();
        if (jCommander.getCommands().containsKey(parsedCommand)) {
            String subCommand = jCommander.getCommands().get(parsedCommand).getParsedCommand();
            return subCommand != null ? subCommand: "";
        } else {
            return "";
        }
    }


    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        // This small hack allow to configure the appropriate Logger level from the command line, this is done
        // by setting the DEFAULT_LOG_LEVEL_KEY before the logger object is created.
//        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, logLevel);
        LogManager.getRootLogger().setLevel(Level.toLevel(logLevel));
        logger = LoggerFactory.getLogger(this.getClass().toString());
        this.logLevel = logLevel;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public String getConfigFile() {
        return configFile;
    }

    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

    public Logger getLogger() {
        return logger;
    }

}
