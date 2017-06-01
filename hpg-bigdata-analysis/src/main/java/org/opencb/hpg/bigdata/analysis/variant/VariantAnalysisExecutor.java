package org.opencb.hpg.bigdata.analysis.variant;

import org.opencb.hpg.bigdata.analysis.AnalysisExecutor;

/**
 * Created by jtarraga on 30/05/17.
 */
public abstract class VariantAnalysisExecutor extends AnalysisExecutor {

    protected String studyName;

    public String getStudyName() {
        return studyName;
    }

    public void setStudyName(String studyName) {
        this.studyName = studyName;
    }
}
