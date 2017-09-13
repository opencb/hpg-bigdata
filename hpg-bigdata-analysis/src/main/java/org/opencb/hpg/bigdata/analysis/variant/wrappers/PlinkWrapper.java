package org.opencb.hpg.bigdata.analysis.variant.wrappers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import htsjdk.variant.variantcontext.writer.Options;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.tools.variant.converters.VCFExporter;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataManager;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.hpg.bigdata.analysis.tools.Executor;
import org.opencb.hpg.bigdata.analysis.tools.Status;
import org.opencb.hpg.bigdata.analysis.tools.ToolManager;
import org.opencb.hpg.bigdata.core.lib.SparkConfCreator;
import org.opencb.hpg.bigdata.core.lib.VariantDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class PlinkWrapper extends VariantAnalysisWrapper {

    private Query query;
    private QueryOptions queryOptions;

    private Logger logger;

    public PlinkWrapper(String studyId) {
        super(studyId);
        this.logger = LoggerFactory.getLogger(PlinkWrapper.class);
    }


    public void execute() {

        String inputAvroFilename = query.getString("input");
        String tmpVcfFilename = query.get("outdir") + "/tmp.vcf";
        String metaFilename = inputAvroFilename + ".meta.json";

        // Generate VCF file by calling VCF exporter from query and query options
        VariantMetadataManager manager = new VariantMetadataManager();
        try {
            manager.load(Paths.get(metaFilename));

            SparkConf sparkConf = SparkConfCreator.getConf("tool plink", "local", 1, true);
            SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));

            VariantDataset vd = new VariantDataset(sparkSession);
            vd.load(inputAvroFilename);
            vd.createOrReplaceTempView("vcf");

            //vd.regionFilter(new Region("22:16050114-16050214"));
            //vd.sampleFilter("GT", "5:0|1");

            // out filename
            VariantStudyMetadata studyMetadata = manager.getVariantMetadata().getStudies().get(0);
            VCFExporter vcfExporter = new VCFExporter(studyMetadata);
            vcfExporter.open(Options.ALLOW_MISSING_FIELDS_IN_HEADER, Paths.get(tmpVcfFilename));

            vcfExporter.export(vd.iterator());

            // close everything
            vcfExporter.close();
            sparkSession.stop();
        } catch (Exception e) {
            logger.error("Error executing PLINK tool when retrieving variants to VCF file: {}", e.getMessage());
            return;
        }

        // Execute PLINK
        try {
            Path tmp = Paths.get("/tmp");
            Path plinkPath = Paths.get("/tmp/plink");
            ToolManager toolManager = new ToolManager(plinkPath);

            Map<String, String> params = new HashMap<>();
            params.put("input", tmpVcfFilename);
            //params.put("output", "/tmp/test.bam.bai");

            String commandLine = toolManager.createCommandLine("plink", "index", params);
            System.out.println(commandLine);

            Executor.execute(commandLine, tmp, true);

            ObjectReader reader = new ObjectMapper().reader(Status.class);
            Status status = reader.readValue(tmp.resolve("status.json").toFile());
        } catch (Exception e) {
            logger.error("Error executing PLINK command line: {}", e.getMessage());
            return;
        }
    }
}
