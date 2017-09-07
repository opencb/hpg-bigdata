package org.opencb.hpg.bigdata.app.cli.local;

import htsjdk.variant.variantcontext.writer.Options;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.tools.variant.converters.VCFExporter;
import org.opencb.biodata.tools.variant.converters.avro.VariantDatasetMetadataToVCFHeaderConverter;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataManager;
import org.opencb.hpg.bigdata.app.cli.local.executors.VariantCommandExecutor;
import org.opencb.hpg.bigdata.core.lib.SparkConfCreator;
import org.opencb.hpg.bigdata.core.lib.VariantDataset;

import java.nio.file.Paths;

/**
 * Created by jtarraga on 12/10/16.
 */
public class VariantQueryCLITest {

    //        String inputFilename = "/tmp/chr8.platinum.avro";
//    String inputFilename = "/home/jtarraga/data150/spark/chr8.platinum.avro";
//    String inputFilename = "/home/jtarraga/data150/spark/chr8.platinum.parquet";
    String inputFilename = "../hpg-bigdata-core/src/test/resources/org/opencb/hpg/bigdata/core/lib/100.variants.avro";
    String outputFilename = "/tmp/query.out";

    public static void execute(String commandLine) throws Exception {
        System.out.println("Executing:\n" + commandLine + "\n");
        LocalCliOptionsParser parser = new LocalCliOptionsParser();
        parser.parse(commandLine.split(" "));

        VariantCommandExecutor executor = new VariantCommandExecutor(parser.getVariantCommandOptions());
        executor.execute();
    }

    @Test
    public void query00() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --type INDEL");
        commandLine.append(" --limit 10");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void query0() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --count");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //    @Test
    public void query1() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --limit 3");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //@Test
    public void query2() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --population-maf \"1000GENOMES_phase_3::ALL<0.01,EXAC::ALL<0.01\"");
        commandLine.append(" --protein-substitution \"sift<0.2\"");
        commandLine.append(" --consequence-type \"missense_variant\"");
        commandLine.append(" --count");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void query3() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --population-maf \"1000GENOMES_phase_3::ALL<0.01,EXAC::ALL<0.01\"");
        commandLine.append(" --protein-substitution \"sift<0.2\"");
        commandLine.append(" --consequence-type \"missense_variant\"");
        commandLine.append(" --gene \"BIN3,ZNF517\"");
        commandLine.append(" --count");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //    @Test
    public void query4() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --population-maf \"1000GENOMES_phase_3::ALL<0.01,EXAC::ALL<0.01\"");
        commandLine.append(" --protein-substitution \"sift<0.2\"");
        commandLine.append(" --consequence-type \"missense_variant\"");
        commandLine.append(" --limit 100");
        commandLine.append(" --group-by gene");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void query5() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --sample-genotype \"0:0|0;3:1|0,1|1\"");
        commandLine.append(" --limit 10");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //    @Test
    public void query6() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --sample-genotype \"0:0|0;3:1|0,1|1\"");
        commandLine.append(" --population-maf \"1000GENOMES_phase_3::ALL<0.01,EXAC::ALL<0.01\"");
        commandLine.append(" --protein-substitution \"sift<0.2\"");
        commandLine.append(" --consequence-type \"missense_variant\"");
        commandLine.append(" --count");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //    @Test
    public void query7() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --sample-genotype \"0:0|0;3:1|0,1|1\"");
        commandLine.append(" --population-maf \"1000GENOMES_phase_3::ALL<0.01,EXAC::ALL<0.01\"");
        commandLine.append(" --protein-substitution \"sift<0.2\"");
        commandLine.append(" --consequence-type \"missense_variant\"");
        commandLine.append(" --gene \"BIN3,ZNF517\"");
        commandLine.append(" --count");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //    @Test
    public void query8() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --sample-genotype \"0:0|0;3:1|0,1|1\"");
        commandLine.append(" --population-maf \"1000GENOMES_phase_3::ALL<0.01,EXAC::ALL<0.01\"");
        commandLine.append(" --protein-substitution \"sift<0.2\"");
        commandLine.append(" --consequence-type \"missense_variant\"");
        commandLine.append(" --limit 100");
        commandLine.append(" --group-by gene");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void query9() {

        inputFilename = "/home/jtarraga/data150/vcf/chr22.head1k.vcf.avro";
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --sample-genotype \"HG00100:1|0,1|1\"");
        commandLine.append(" --limit 100");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void query10() {
        VariantMetadataCLITest metaCLI = new VariantMetadataCLITest();
        metaCLI.loadPedigree();

        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(metaCLI.avroPath);
        commandLine.append(" --sample-genotype \"5:1|0\"");
        //commandLine.append(" --sample-filter \"Eyes==Green\"");
        commandLine.append(" --limit 100");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void query11() {
        VariantMetadataCLITest metaCLI = new VariantMetadataCLITest();
        metaCLI.loadPedigree();

        VariantDatasetMetadataToVCFHeaderConverter headerConverter = new VariantDatasetMetadataToVCFHeaderConverter();
        VariantMetadataManager manager = new VariantMetadataManager();
        try {
            manager.load(metaCLI.metadataPath);

            SparkConf sparkConf = SparkConfCreator.getConf("variant query", "local", 1, true);
            SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));

            VariantDataset vd = new VariantDataset(sparkSession);
            vd.load(metaCLI.avroPath.toString());
            vd.createOrReplaceTempView("vcf");
            vd.regionFilter(new Region("22:16050114-16050214"));

            // out filename
            String outFilename = metaCLI.metadataPath.toString() + ".vcf";

            VariantStudyMetadata studyMetadata = manager.getVariantMetadata().getStudies().get(0);
            VCFExporter vcfExporter = new VCFExporter(studyMetadata);
            vcfExporter.open(Options.ALLOW_MISSING_FIELDS_IN_HEADER, Paths.get(outFilename));

            vcfExporter.export(vd.iterator());

            // close everything
            vcfExporter.close();
            sparkSession.stop();

/*
            String datasetId = manager.getVariantMetadata().getStudies().get(0).getId();
            VCFHeader vcfHeader = headerConverter.convert(manager.getVariantMetadata().getStudies().get(0));

            // create the variant context writer
            String outFilename = metaCLI.metadataPath.toString() + ".vcf";
            OutputStream outputStream = new FileOutputStream(outFilename);
            Options writerOptions = null;
            VariantContextWriter writer = VcfUtils.createVariantContextWriter(outputStream,
                    vcfHeader.getSequenceDictionary());//, writerOptions);

            // write VCF header
            writer.writeHeader(vcfHeader);



            ObjectMapper mapper = new ObjectMapper();
            List<String> samples = VariantMetadataUtils.getSampleNames(manager.getVariantMetadata().getStudies().get(0));
            List<String> formats = Arrays.asList("GT");
            List<String> annotations = new ArrayList<>();
            VariantAvroToVariantContextConverter converter = new VariantAvroToVariantContextConverter(datasetId, samples, formats, annotations);
/*
            List<String> list = vd.toJSON().collectAsList();
            for (String item: list) {
                Variant variant = mapper.readValue(item, Variant.class);
                System.out.println(variant.getId() + ", " + variant.getChromosome() + ", " + variant.getStart());
                VariantContext variantContext = converter.convert(variant);
                writer.add(variantContext);
            }
*/
/*
            Iterator<String> iterator = vd.toJSON().toLocalIterator();

            VariantStudyMetadata studyMetadata = manager.getVariantMetadata().getStudies().get(0);
            VCFExporter vcfExporter = new VCFExporter(studyMetadata);
            SparkVariantIterator variantIterator = new SparkVariantIterator(vd);
            vcfExporter.export(variantIterator, null, outFilename);

            while (iterator.hasNext()) {
                Variant variant = mapper.readValue(iterator.next(), Variant.class);
                System.out.println(">>> writing to " + outFilename);
                System.out.println(variant.getId() + ", " + variant.getChromosome() + ", " + variant.getStart());
                System.out.println(variant.toJson());
                VariantContext variantContext = converter.convert(variant);
                for (int i = 0; i < 6; i++) {
                    System.out.println("\t" + variantContext.getGenotype(i));
                }
                writer.add(variantContext);
            }
/*
            vd.toJSON().foreach(s -> {
                Variant variant = mapper.readValue(s, Variant.class);
                System.out.println(variant.getId() + ", " + variant.getChromosome() + ", " + variant.getStart());
                VariantContext variantContext = converter.from(variant);
                writer.add(variantContext);
            });
            /*
            javaRDD().foreach(row -> {

                writer.add(null);
                System.out.println(row.get(0) + ", " + row.get(1) + ", " + row.get(2) + ", " + row.get(3));
            });

            // close everything
            sparkSession.stop();
            writer.close();
            outputStream.close();
*/
        } catch (Exception e) {
            e.printStackTrace();
        }

/*
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(metaCLI.avroPath);
        commandLine.append(" --limit 100");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    */
    }
}
