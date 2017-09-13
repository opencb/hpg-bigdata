package org.opencb.hpg.bigdata.analysis.variant.wrappers;

import org.opencb.hpg.bigdata.analysis.variant.VariantAnalysisExecutor;

import java.nio.file.Path;

public abstract class VariantAnalysisWrapper extends VariantAnalysisExecutor {

    protected Path binPath;

    protected VariantAnalysisWrapper(String studyId) {
        super(studyId);
    }

    public void setBinPath(Path binPath) {
        this.binPath = binPath;
    }

    public Path getBinPath() {
        return binPath;
    }
}
