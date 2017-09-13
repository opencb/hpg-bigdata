package org.opencb.hpg.bigdata.analysis.variant;

import org.opencb.hpg.bigdata.analysis.AnalysisExecutor;

/**
 * Created by jtarraga on 30/05/17.
 */
public abstract class VariantAnalysisExecutor extends AnalysisExecutor {

    protected VariantAnalysisExecutor(String studyId) {
        super(studyId);
    }
}
