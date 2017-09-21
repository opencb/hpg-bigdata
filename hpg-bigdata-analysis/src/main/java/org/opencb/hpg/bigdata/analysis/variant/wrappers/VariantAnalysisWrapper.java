package org.opencb.hpg.bigdata.analysis.variant.wrappers;

import org.opencb.hpg.bigdata.analysis.variant.VariantAnalysisExecutor;

import java.nio.file.Path;

public abstract class VariantAnalysisWrapper extends VariantAnalysisExecutor {
    protected Path binPath;

    protected VariantAnalysisWrapper(String studyId) {
        super(studyId);
    }

    protected VariantAnalysisWrapper(String studyId, Path binPath) {
        super(studyId);
        this.binPath = binPath;
    }

    public VariantAnalysisWrapper setBinPath(Path binPath) {
        this.binPath = binPath;
        return this;
    }

    public Path getBinPath() {
        return binPath;
    }
}
