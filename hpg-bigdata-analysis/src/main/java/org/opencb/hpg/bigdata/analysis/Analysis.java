package org.opencb.hpg.bigdata.analysis;

/**
 * Created by jtarraga on 30/05/17.
 */
public abstract class Analysis {

    protected String datasetName;
    protected String studyName;

    public abstract void run();

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public String getStudyName() {
        return studyName;
    }

    public void setStudyName(String studyName) {
        this.studyName = studyName;
    }
}
