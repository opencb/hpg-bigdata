package org.opencb.hpg.bigdata.analysis.variant;

import org.opencb.hpg.bigdata.analysis.AnalysisExecutor;
import org.opencb.hpg.bigdata.core.config.OskarConfiguration;

/**
 * Created by jtarraga on 30/05/17.
 */
public abstract class VariantAnalysisExecutor extends AnalysisExecutor {

    protected VariantAnalysisExecutor(String studyId, OskarConfiguration configuration) {
        super(studyId, configuration);
    }
}
