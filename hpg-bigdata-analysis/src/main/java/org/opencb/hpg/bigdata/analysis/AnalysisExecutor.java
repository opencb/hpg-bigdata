package org.opencb.hpg.bigdata.analysis;

/**
 * Created by jtarraga on 30/01/17.
 */
public abstract class AnalysisExecutor {
    protected String studyId;

    protected AnalysisExecutor(String studyId) {
        this.studyId = studyId;
    }

    protected String studyId() {
        return studyId;
    }

    protected void setStudyId(String studyId) {
        this.studyId = studyId;
    }

    protected abstract void execute() throws AnalysisExecutorException;
}
