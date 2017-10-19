package org.opencb.hpg.bigdata.analysis.variant.wrappers;

import org.opencb.hpg.bigdata.analysis.variant.VariantAnalysisExecutor;
import org.opencb.hpg.bigdata.core.config.OskarConfiguration;

public abstract class VariantAnalysisWrapper extends VariantAnalysisExecutor {

    protected VariantAnalysisWrapper(String studyId, OskarConfiguration configuration) {
        super(studyId, configuration);
    }
}
