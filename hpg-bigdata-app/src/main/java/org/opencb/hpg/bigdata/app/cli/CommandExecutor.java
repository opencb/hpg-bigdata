package org.opencb.hpg.bigdata.app.cli;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by imedina on 03/02/15.
 */
public abstract class CommandExecutor {

    protected String logLevel;
    protected boolean verbose;
    protected String configFile;

    protected String appHome;

    protected Logger logger;

    public CommandExecutor() {

    }

    public CommandExecutor(String logLevel, boolean verbose, String configFile) {
        this.logLevel = logLevel;
        this.verbose = verbose;
        this.configFile = configFile;

        /**
         * System property 'app.home' is set up by cellbase.sh. If by any reason this is null
         * then CELLBASE_HOME environment variable is used instead.
         */
        this.appHome = System.getProperty("app.home", "/opt/bigdata");

        if(logLevel != null && !logLevel.isEmpty()) {
            // We must call to this method
            setLogLevel(logLevel);
        }
    }

    public abstract void parse();

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
