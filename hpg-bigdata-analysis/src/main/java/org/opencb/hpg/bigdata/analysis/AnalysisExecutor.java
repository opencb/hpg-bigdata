package org.opencb.hpg.bigdata.analysis;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jtarraga on 30/01/17.
 */
public abstract class AnalysisExecutor {

    protected static Logger logger = LoggerFactory.getLogger(AnalysisExecutor.class);

    public static String metadataExtension = ".meta.json";

    protected String datasetName;
    protected SparkSession sparkSession;

    protected abstract void execute() throws AnalysisExecutorException;

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void setSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }
}
