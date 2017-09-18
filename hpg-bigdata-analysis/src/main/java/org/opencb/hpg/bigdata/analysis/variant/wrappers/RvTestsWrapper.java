package org.opencb.hpg.bigdata.analysis.variant.wrappers;

import org.opencb.hpg.bigdata.analysis.AnalysisExecutorException;
import org.opencb.hpg.bigdata.analysis.exceptions.AnalysisToolException;
import org.opencb.hpg.bigdata.analysis.tools.Executor;
import org.opencb.hpg.bigdata.analysis.variant.FilterParameters;
import org.opencb.hpg.bigdata.analysis.variant.VariantAnalysisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Created by joaquin on 1/19/17.
 */
//public class RvTestsWrapper extends AnalysisExecutor implements Serializable {
public class RvTestsWrapper extends VariantAnalysisWrapper { // extends AnalysisExecutor implements Serializable {
    private String inFilename;
    private String metaFilename;
    private FilterParameters filterParams;
    private Map<String, String> rvtestsParams;

    private Logger logger;

    public RvTestsWrapper(String studyId, String inFilename, String metaFilename,
                          FilterParameters filterParams, Map<String, String> rvtestsParams) {
        super(studyId);
        this.inFilename = inFilename;
        this.metaFilename = metaFilename;
        this.filterParams = filterParams;
        this.rvtestsParams = rvtestsParams;

        this.logger = LoggerFactory.getLogger(PlinkWrapper.class);
    }

    @Override
    public void execute() throws AnalysisExecutorException {
        // Sanity chek
        if (binPath == null || !binPath.toFile().exists()) {
            String msg = "RvTests binary path is missing or does not exist:  '" + binPath + "'.";
            logger.error(msg);
            throw new AnalysisExecutorException(msg);
        }

        // Get output dir
        Path outDir = Paths.get("/tmp");
        if (rvtestsParams.get("out") != null) {
            outDir = Paths.get(rvtestsParams.get("out")).getParent();
        }

        // Export target variants to VCF file
        String vcfFilename = outDir.toString() + "/tmp.vcf";
        VariantAnalysisUtils.exportVCF(inFilename, metaFilename, filterParams, vcfFilename);

        // Export pedigree
        String pedFilename = outDir.toString() + "/tmp.vcf.ped";
        try {
            VariantAnalysisUtils.exportPedigree(metaFilename, studyId, pedFilename);
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new AnalysisExecutorException(e);
        }

        StringBuilder sb = new StringBuilder();
/*
        // Compress vcf to bgz
        sb.setLength(0);
        sb.append(BGZIP_BIN).append(" ").append(vcfFilename);
        Executor.execute(sb.toString(), outDir, true);

        // ...create tabix index
        sb.setLength(0);
        sb.append(TABIX_BIN).append(" -p vcf ").append(vcfFilename).append(".gz");
        Executor.execute(sb.toString(), outDir, true);
*/
        // ...and finally, run rvtests
        sb.setLength(0);
        sb.append(binPath);
        sb.append(" --inVcf ").append(vcfFilename).append(".gz");
        sb.append(" --pheno ").append(pedFilename);
        for (String key: rvtestsParams.keySet()) {
            sb.append(" --").append(key).append(" ").append(rvtestsParams.get(key));
        }
        try {
            Executor.execute(sb.toString(), outDir, true);
        } catch (AnalysisToolException e) {
            logger.error(e.getMessage());
            throw new AnalysisExecutorException(e);
        }
    }
}
