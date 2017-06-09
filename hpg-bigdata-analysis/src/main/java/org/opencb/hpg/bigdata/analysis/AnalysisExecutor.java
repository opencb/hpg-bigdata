package org.opencb.hpg.bigdata.analysis;

import org.apache.spark.sql.SparkSession;

/**
 * Created by jtarraga on 30/01/17.
 */
public abstract class AnalysisExecutor {

    public static String metadataExtension = ".meta.json";

    protected String datasetName;
    protected SparkSession sparkSession;

    public abstract void execute() throws AnalysisExecutorException;

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
