package org.opencb.hpg.bigdata.analysis;

/**
 * Created by jtarraga on 30/01/17.
 */
public abstract class AnalysisExecutor {
    protected String datasetName;

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public abstract void execute() throws AnalysisExecutorException;
}
