package org.opencb.hpg.bigdata.tools.variant.analysis;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.opencb.biodata.formats.pedigree.PedigreeManager;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.core.pedigree.Pedigree;
import org.opencb.biodata.models.variant.VariantMetadataManager;
import org.opencb.commons.utils.FileUtils;
import org.opencb.hpg.bigdata.core.lib.SparkConfCreator;
import org.opencb.hpg.bigdata.core.lib.VariantDataset;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

/**
 * Created by joaquin on 1/19/17.
 */
public class RvTestsAnalysis {
    private String inFilename;
    private String outDirname;
    private String confFilename;

    private final String RVTEST_BIN = "/home/jtarraga/softs/rvtests/executable/rvtest";
    private final String BGZIP_BIN = "/home/joaquin/softs/htslib/bgzip";
    private final String TABIX_BIN = "/home/joaquin/softs/htslib/tabix";

    public RvTestsAnalysis(String inFilename, String outDirname, String confFilename) {
        this.inFilename = inFilename;
        this.outDirname = outDirname;
        this.confFilename = confFilename;
    }

//    ./build/bin/hpg-bigdata-local2.sh variant rvtests -i ~/data/vcf/skat/example.vcf.avro -o ~/data/vcf/skat/out
// --dataset noname -c ~/data/vcf/skat/skat.params

    public void run(String dataset) throws Exception {
        // create spark session
        SparkConf sparkConf = SparkConfCreator.getConf("variant rvtests", "local", 1, true);
        SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));

        // load dataset
        VariantDataset vd = new VariantDataset(sparkSession);
        vd.load(inFilename);
        vd.createOrReplaceTempView("vcf");

        // load rvtests parameters
        Properties prop = new Properties();
        InputStream confStream = new FileInputStream(confFilename);
        prop.load(confStream);
        confStream.close();

        for (Object key: prop.keySet()) {
            System.out.println((String) key + " = " + (String) prop.get(key));
        }

        // create temporary directory
        File tmpDir = new File(outDirname + "/tmp");
        tmpDir.mkdir();

        // create temporary file for --pheno
        File phenoFile = new File(tmpDir.getAbsolutePath() + "/pheno");
        VariantMetadataManager metadataManager = new VariantMetadataManager();
        metadataManager.load(inFilename + ".meta.json");
        Pedigree pedigree = metadataManager.getPedigree(dataset);
        new PedigreeManager().save(pedigree, phenoFile.toPath());

        // loop for regions
        String line;
        BufferedReader reader = FileUtils.newBufferedReader(Paths.get(prop.getProperty("setFile")));
        int i = 0;
        StringBuilder cmdline = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            String[] fields = line.split("[\t ]");
            System.out.println(fields[0]);
            String regionName = fields[0];
            Region region = new Region(fields[1]);

            // create temporary files for --inVcf and --setFile
            File setFile = new File(tmpDir.getAbsolutePath() + "/setFile." + i);
            BufferedWriter writer = FileUtils.newBufferedWriter(setFile.toPath());
            writer.write(fields[0] + "\t" + fields[1] + "\n");
            writer.close();

            // create temporary vcf file fot the region variants
            VariantDataset ds = (VariantDataset) vd.regionFilter(region);
            File vcfFile = new File(tmpDir.getAbsolutePath() + "/variants." + i + ".vcf");
            writer = FileUtils.newBufferedWriter(vcfFile.toPath());
            writer.write("##fileformat=VCFv4.2\n");
            writer.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT");
            for (String key: pedigree.getIndividuals().keySet()) {
                writer.write("\t");
                writer.write(pedigree.getIndividuals().get(key).getId());
            }
            writer.write("\n");
            List<Row> rows = ds.collectAsList();
            for (Row row: rows) {
                System.out.println(row);
            }
            writer.close();

            // compress vcf to bgz
            cmdline.setLength(0);
            cmdline.append(this.BGZIP_BIN).append(" ").append(vcfFile.getAbsolutePath());
            Process p = execute(cmdline.toString());
            System.out.println("Compressing vcf to gz: " + cmdline);
            System.out.println("\tSTDOUT:");
            System.out.println(readInputStream(p.getInputStream()));
            System.out.println("\tSTDERR:");
            System.out.println(readInputStream(p.getErrorStream()));

            // and create tabix index
            cmdline.setLength(0);
            cmdline.append(this.TABIX_BIN).append(" -p vcf ").append(vcfFile.getAbsolutePath()).append(".gz");
            p = execute(cmdline.toString());
            System.out.println("Creating tabix index: " + cmdline);
            System.out.println("\tSTDOUT:");
            System.out.println(readInputStream(p.getInputStream()));
            System.out.println("\tSTDERR:");
            System.out.println(readInputStream(p.getErrorStream()));

            // rvtests command line
            cmdline.setLength(0);
            cmdline.append(this.RVTEST_BIN).append(" --kernel skat --pheno ").append(phenoFile.getAbsolutePath())
                    .append(" --inVcf ").append(vcfFile.getAbsolutePath()).append(".gz")
                    .append(" --setFile ").append(setFile.getAbsolutePath())
                    .append(" --out ").append(tmpDir.getAbsolutePath()).append("/out.").append(i);
            p = execute(cmdline.toString());
            System.out.println("Execute test: " + cmdline);
            System.out.println("\tSTDOUT:");
            System.out.println(readInputStream(p.getInputStream()));
            System.out.println("\tSTDERR:");
            System.out.println(readInputStream(p.getErrorStream()));

            i++;
        }
        reader.close();
    }

    private Process execute(String cmdline) {
        Process p = null;
        try {
            System.out.println("Executing: " + cmdline);
            p = Runtime.getRuntime().exec(cmdline);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return p;
    }

    private void saveStream(InputStream inputStream, Path path) {
        try {
            PrintWriter writer = new PrintWriter(path.toFile());
            writer.write(readInputStream(inputStream));
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String readInputStream(InputStream inputStream) {
        StringBuilder res = new StringBuilder();
        try {
            // read the output from the command
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(inputStream));
            String s;
            System.out.println("Here is the standard output of the command:\n");
            while ((s = stdInput.readLine()) != null) {
                res.append(s).append("\n");
            }
        } catch (IOException e) {
            res.append(e.getMessage()).append("\n");
        }
        return res.toString();
    }
}
