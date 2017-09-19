package org.opencb.hpg.bigdata.analysis.variant.wrappers;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.opencb.biodata.models.metadata.Individual;
import org.opencb.biodata.models.metadata.Sample;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataManager;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataUtils;
import org.opencb.hpg.bigdata.analysis.AnalysisExecutorException;
import org.opencb.hpg.bigdata.analysis.exceptions.AnalysisToolException;
import org.opencb.hpg.bigdata.analysis.tools.Executor;
import org.opencb.hpg.bigdata.analysis.tools.ToolManager;
import org.opencb.hpg.bigdata.analysis.variant.FilterParameters;
import org.opencb.hpg.bigdata.core.lib.SparkConfCreator;
import org.opencb.hpg.bigdata.core.lib.VariantDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PlinkWrapper extends VariantAnalysisWrapper {
    private String inFilename;
    private String metaFilename;
    private FilterParameters filterParams;
    private Map<String, String> plinkParams;

    private Logger logger;

    public PlinkWrapper(String studyId, String inFilename, String metaFilename,
                        FilterParameters filterParams, Map<String, String> plinkParams) {
        super(studyId);
        this.inFilename = inFilename;
        this.metaFilename = metaFilename;
        this.filterParams = filterParams;
        this.plinkParams = plinkParams;

        this.logger = LoggerFactory.getLogger(PlinkWrapper.class);
    }


    public void execute() throws AnalysisExecutorException {
        // Sanity check
        if (binPath == null || !binPath.toFile().exists()) {
            String msg = "PLINK binary path is missing or does not exist:  '" + binPath + "'.";
            logger.error(msg);
            throw new AnalysisExecutorException(msg);
        }

        // Get output dir
        Path outDir = Paths.get("/tmp");
        if (plinkParams.get("out") != null) {
            outDir = Paths.get(plinkParams.get("out")).getParent();
        }

        // Generate VCF file by calling VCF exporter from query and query options
        VariantMetadataManager manager = new VariantMetadataManager();
        try {
            manager.load(Paths.get(metaFilename));

            SparkConf sparkConf = SparkConfCreator.getConf("PLINK", "local", 1, true);
            SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));

            VariantDataset vd = new VariantDataset(sparkSession);
            vd.load(inFilename);
            vd.createOrReplaceTempView("vcf");

            //vd.regionFilter(new Region("22:16050114-16050214"));
            //vd.sampleFilter("GT", "5:0|1");

            // out filename
            VariantStudyMetadata studyMetadata = manager.getVariantMetadata().getStudies().get(0);
            exportPedMapFile(vd, studyMetadata, outDir + "/plink");

            // close
            sparkSession.stop();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error executing PLINK tool when retrieving variants to PED and MAP files: {}", e.getMessage());
            return;
        }

        // Execute PLINK
        StringBuilder sb = new StringBuilder();
        sb.append(binPath);
        sb.append(" --file ").append(outDir).append("/plink");
        for (String key : plinkParams.keySet()) {
            sb.append(" --").append(key);
            String value = plinkParams.get(key);
            if (!StringUtils.isEmpty(value)) {
                sb.append(" ").append(value);
            }
        }
        try {
            Executor.execute(sb.toString(), outDir, true);
        } catch (AnalysisToolException e) {
            logger.error(e.getMessage());
            throw new AnalysisExecutorException(e);
        }
    }

    public void exportPedMapFile(VariantDataset variantDataset, VariantStudyMetadata studyMetadata,
                                 String prefix) throws FileNotFoundException {
        Path pedPath = Paths.get(prefix + ".ped");
        Path mapPath = Paths.get(prefix + ".map");

        StringBuilder sb = new StringBuilder();
        PrintWriter pedWriter = new PrintWriter(pedPath.toFile());
        PrintWriter mapWriter = new PrintWriter(mapPath.toFile());
        Iterator<Variant> iterator = variantDataset.iterator();

        List<String> sampleNames = VariantMetadataUtils.getSampleNames(studyMetadata);
        //List<String> ms = new ArrayList<>(sampleNames.size());
        StringBuilder[] markers = new StringBuilder[sampleNames.size()];
        while (iterator.hasNext()) {
            Variant variant = iterator.next();
            // genotypes
            List<List<String>> sampleData = variant.getStudiesMap().get(studyMetadata.getId()).getSamplesData();
            assert(sampleData.size() == sampleNames.size());
            for (int i = 0; i < sampleData.size(); i++) {
                String[] gt = sampleData.get(i).get(0).split("[|/]");
                if (markers[i] == null) {
                    markers[i] = new StringBuilder();
                }
                //if (markers.get(i) == null) {
                //    markers.set(i, "");
                //}
                //markers[i].set(i, markers.get(i) + "\t" + gt[0] + "\t" + gt[1]);
                markers[i].append("\t"
                        + (gt[0].equals("1") ? variant.getAlternate() : variant.getReference())
                        + "\t"
                        + (gt[1].equals("1") ? variant.getAlternate() : variant.getReference()));
            }

            // map file line
            mapWriter.println(variant.getChromosome() + "\t" + variant.getId() + "\t0\t" + variant.getStart());
        }

        // ped file line
        for (int i = 0; i < sampleNames.size(); i++) {
            sb.setLength(0);
            String sampleName = sampleNames.get(i);
            Individual individual = getIndividualBySampleName(sampleName, studyMetadata);
            if (individual == null) {
                // sample not found, what to do??
                sb.append(0).append("\t");
                sb.append(sampleName).append("\t");
                sb.append(0).append("\t");
                sb.append(0).append("\t");
                sb.append(0).append("\t");
                sb.append(0);
            } else {
                int sex = org.opencb.biodata.models.core.pedigree.Individual.Sex
                        .getEnum(individual.getSex()).getValue();
                int phenotype = org.opencb.biodata.models.core.pedigree.Individual.AffectionStatus
                        .getEnum(individual.getPhenotype()).getValue();
                sb.append(individual.getFamily() == null ? 0 : individual.getFamily()).append("\t");
                sb.append(sampleName).append("\t");
                sb.append(individual.getFather() == null ? 0 : individual.getFather()).append("\t");
                sb.append(individual.getMother() == null ? 0 : individual.getMother()).append("\t");
                sb.append(sex).append("\t");
                sb.append(phenotype);
            }
            sb.append(markers[i]);
            pedWriter.println(sb.toString());
        }

        // close
        pedWriter.close();
        mapWriter.close();
    }


    private Individual getIndividualBySampleName(String sampleName, VariantStudyMetadata studyMetadata) {
        for (Individual individual: studyMetadata.getIndividuals()) {
            for (Sample sample: individual.getSamples()) {
                if (sampleName.equals(sample.getId())) {
                    return individual;
                }
            }
        }
        return null;
    }
}
