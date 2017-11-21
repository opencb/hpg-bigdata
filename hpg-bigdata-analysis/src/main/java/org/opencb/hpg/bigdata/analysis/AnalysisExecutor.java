package org.opencb.hpg.bigdata.analysis;

import org.opencb.hpg.bigdata.analysis.exceptions.AnalysisExecutorException;
import org.opencb.hpg.bigdata.core.config.OskarConfiguration;

/**
 * Created by jtarraga on 30/01/17.
 */
public abstract class AnalysisExecutor {
    protected String studyId;
    protected OskarConfiguration configuration;

    protected AnalysisExecutor(String studyId, OskarConfiguration configuration) {
        this.studyId = studyId;
        this.configuration = configuration;
    }

    protected abstract void execute() throws AnalysisExecutorException;

    @Override
    public String toString() {
        return "AnalysisExecutor{" + "studyId='" + studyId + '\'' + ", configuration=" + configuration + '}';
    }

    public String getStudyId() {
        return studyId;
    }

    public AnalysisExecutor setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public OskarConfiguration getConfiguration() {
        return configuration;
    }

    public AnalysisExecutor setConfiguration(OskarConfiguration configuration) {
        this.configuration = configuration;
        return this;
    }
}
