package org.opencb.hpg.bigdata.analysis.variant.adaptors;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.opencb.biodata.formats.pedigree.PedigreeManager;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.core.pedigree.Pedigree;
import org.opencb.biodata.models.variant.VariantMetadataManager;
import org.opencb.commons.utils.FileUtils;
import org.opencb.hpg.bigdata.analysis.AnalysisExecutor;
import org.opencb.hpg.bigdata.analysis.AnalysisExecutorException;
import org.opencb.hpg.bigdata.core.lib.SparkConfCreator;
import org.opencb.hpg.bigdata.core.lib.VariantDataset;
import scala.Tuple2;
import scala.collection.mutable.StringBuilder;
import scala.collection.mutable.WrappedArray;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by joaquin on 1/19/17.
 */
public class RvTestsAdaptor extends AnalysisExecutor implements Serializable {
    private String inFilename;
    private String metaFilename;
    private String outDirname;
    private String confFilename;

    private final String RVTEST_BIN = "rvtests"; //"/home/jtarraga/soft/rvtests/executable/rvtest";
    private final String BGZIP_BIN = "bgzip"; //"/home/joaquin/softs/htslib/bgzip";
    private final String TABIX_BIN = "tabix"; //"/home/joaquin/softs/htslib/tabix";

    public RvTestsAdaptor(String inFilename, String outDirname, String confFilename) {
        this(inFilename, inFilename + ".meta.json", outDirname, confFilename);
    }

    public RvTestsAdaptor(String inFilename, String metaFilename, String outDirname, String confFilename) {
        this.inFilename = inFilename;
        this.metaFilename = metaFilename;
        this.outDirname = outDirname;
        this.confFilename = confFilename;
    }

//    ./build/bin/hpg-bigdata-local2.sh variant rvtests -i ~/data/vcf/skat/example.vcf.avro -o ~/data/vcf/skat/out
// --dataset noname -c ~/data/vcf/skat/skat.params

    public void run(String datasetName) throws Exception {
        // create spark session
        SparkConf sparkConf;
        if (inFilename.startsWith("/")) {
            sparkConf = SparkConfCreator.getConf("variant rvtests", "local", 1, true);
        } else {
            sparkConf = new SparkConf().setAppName("variant rvtests");
        }
        SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));

        // load dataset
        VariantDataset vd = new VariantDataset(sparkSession);
        vd.load(inFilename);
        vd.createOrReplaceTempView("vcf");

        // load rvtests parameters into properties
        Properties props = new Properties();
        InputStream confStream = new FileInputStream(confFilename);
        props.load(confStream);
        confStream.close();

        for (Object key: props.keySet()) {
            System.out.println((String) key + " = " + (String) props.get(key));
        }

        List<String> kernels = null;
        String kernel = props.getProperty("kernel");
        if (kernel != null) {
            kernels = Arrays.asList(kernel.toLowerCase()
                    .replace("skato", "SkatO")
                    .replace("skat", "Skat")
                    .split(","));
        }

        // create temporary directory
        File tmpDir = new File(outDirname + "/tmp");
        tmpDir.mkdir();

        // create temporary file for --pheno
        File phenoFile = new File(tmpDir.getAbsolutePath() + "/pheno");
        VariantMetadataManager metadataManager = new VariantMetadataManager();
        metadataManager.load(metaFilename);
        Pedigree pedigree = metadataManager.getPedigree(datasetName);
        new PedigreeManager().save(pedigree, phenoFile.toPath());

        // loop for regions
        String line;
        BufferedWriter writer =  null;
        BufferedReader reader = FileUtils.newBufferedReader(Paths.get(props.getProperty("setFile")));
        int i = 0;
        StringBuilder sb = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            String[] fields = line.split("[\t ]");
            System.out.println(fields[0]);
            Region region = new Region(fields[1]);

            // create temporary files for --inVcf and --setFile
            File setFile = new File(tmpDir.getAbsolutePath() + "/setFile." + i);
            writer = FileUtils.newBufferedWriter(setFile.toPath());
            writer.write(fields[0] + "\t" + fields[1] + "\n");
            writer.close();

            // create temporary vcf file fot the region variants
            VariantDataset ds = (VariantDataset) vd.regionFilter(region);
            File vcfFile = new File(tmpDir.getAbsolutePath() + "/variants." + i + ".vcf");
            writer = FileUtils.newBufferedWriter(vcfFile.toPath());

            // header lines
            writer.write("##fileformat=VCFv4.2\n");
            writer.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT");
            // sample names
            for (String key: pedigree.getIndividuals().keySet()) {
                writer.write("\t");
                writer.write(pedigree.getIndividuals().get(key).getId());
            }
            writer.write("\n");

            // variant lines
//            writer.write(i + " >>>>>>>>>>>>>> " + line + ", num. rows = " + ds.count() + "\n");
//            writer.close();
//            System.out.println("\n\n" + i + " >>>>>>>>>>>>>> " + line + ", num. rows = " + ds.count() + "\n");
//            ds.show(1, false);
//            ds.reset();

            //System.out.println("----------> !!!! rows should be sorted by start !!!" + ds.getSql());
//            ds.executeSql(ds.getSql()).show();
//            System.out.println("----------> rows should be sorted by start !!!");
//            ds.executeSql("SELECT * FROM vcf  WHERE (chromosome = '22' AND start >= 32755892 AND end <= 32767063)"
//                    + " ORDER BY start ASC").show();
//            System.exit(0);

            //List<Row> rows = ds.collectAsList();
            List<Row> rows = ds.executeSql(ds.getSql()).collectAsList();
            ds.reset();

            for (Row row: rows) {
                List<Row> studies = row.getList(12);
                Row study = null;
                for (int j = 0; j < studies.size(); j++) {
                    if (studies.get(j).get(0).equals(datasetName)) {
                        study = studies.get(j);
                        break;
                    }
                }

                if (study == null) {
                    throw new Exception("Dataset '" + datasetName + "' not found!");
                }

//              System.out.println("\n\n\n" + row.mkString() + "\n\n");
                List<Row> files = study.getList(1);
                String filter = (String) files.get(0).getJavaMap(2).get("FILTER");
                String qual = (String) files.get(0).getJavaMap(2).get("QUAL");

                sb.setLength(0);
                sb.append(row.get(2)).append("\t").append(row.get(3)).append("\t")
                        .append(row.get(0) == null ? "." : row.get(0)).append("\t")
                        .append(row.get(5)).append("\t").append(row.get(6)).append("\t").append(qual).append("\t")
                        .append(filter).append("\t").append(".").append("\t").append(study.getList(3).get(0));

                int j = 0;
                try {
//                    System.out.println("size for (" + i + "): " + sb.toString());
//                    System.out.println("\tsize = " + study.getList(4).size());
                    for (j = 0; j < pedigree.getIndividuals().size(); j++) {
//                        System.out.println(j + ": ");
//                        System.out.println("\t" + ((WrappedArray) study.getList(4).get(j)).head());
                        sb.append("\t").append(((WrappedArray) study.getList(4).get(j)).head());
                    }
                } catch (Exception ex) {
                    System.out.println("Exception for sample data: " + sb.toString());
                    System.out.println("(i, j, sample data size, pedigree size)  = ("
                            + i + ", " + j + ", " + study.getList(4).size() + ", " + pedigree.getIndividuals().size());
                    System.out.println(ex.getMessage());
                    ex.printStackTrace();
                }
                sb.append("\n");
                writer.write(sb.toString());
//                System.out.println(sb.toString());
            }
            writer.close();

            // compress vcf to bgz
            sb.setLength(0);
            sb.append(props.getProperty("bgzip", this.BGZIP_BIN)).append(" ").append(vcfFile.getAbsolutePath());
            execute(sb.toString(), "Compressing vcf to gz: " + sb.toString());

            // and create tabix index
            sb.setLength(0);
            sb.append(props.getProperty("tabix", this.TABIX_BIN)).append(" -p vcf ").append(vcfFile.getAbsolutePath()).append(".gz");
            execute(sb.toString(), "Creating tabix index: " + sb);

            // rvtests command line
            sb.setLength(0);
            sb.append(props.getProperty("rvtests", this.RVTEST_BIN))
                    .append(kernel == null ? " " : " --kernel " + kernel)
                    .append(" --pheno ").append(phenoFile.getAbsolutePath())
                    .append(" --inVcf ").append(vcfFile.getAbsolutePath()).append(".gz")
                    .append(" --setFile ").append(setFile.getAbsolutePath())
                    .append(" --out ").append(tmpDir.getAbsolutePath()).append("/out.").append(i);
            execute(sb.toString(), "Running rvtests: " + sb);

            i++;
        }

        // write final
        for (String k: kernels) {
            boolean header = false;
            writer = FileUtils.newBufferedWriter(Paths.get(outDirname + "/out." + k + ".assoc"));
            for (int j = 0; j < i; j++) {
                File file = new File(tmpDir.getAbsolutePath() + "/out." + j + "." + k + ".assoc");
                if (file.exists()) {
                    reader = FileUtils.newBufferedReader(file.toPath());
                    line = reader.readLine();
                    if (!header) {
                        writer.write(line);
                        writer.write("\n");
                        header = true;
                    }
                    while ((line = reader.readLine()) != null) {
                        writer.write(line);
                        writer.write("\n");
                    }
                    reader.close();
                }
            }
            writer.close();
        }
    }

    public void run00(String datasetName) throws Exception {
        // create spark session
        SparkConf sparkConf;
        if (inFilename.startsWith("/")) {
            sparkConf = SparkConfCreator.getConf("variant rvtests", "local", 1, true);
        } else {
            sparkConf = new SparkConf().setAppName("variant rvtests");
        }
        SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));

        // load dataset
        VariantDataset vd = new VariantDataset(sparkSession);
        vd.load(inFilename);
        vd.createOrReplaceTempView("vcf");

        // load rvtests parameters into properties
        Properties props = new Properties();
        InputStream confStream = new FileInputStream(confFilename);
        props.load(confStream);
        confStream.close();

        List<String> kernels = null;
        String kernel = props.getProperty("kernel");
        if (kernel != null) {
            kernels = Arrays.asList(kernel.toLowerCase()
                    .replace("skato", "SkatO")
                    .replace("skat", "Skat")
                    .split(","));
        }

        // create temporary directory
        File tmpDir = new File(outDirname + "/tmp");
        tmpDir.mkdir();

        // create temporary file for --pheno
        File phenoFile = new File(tmpDir.getAbsolutePath() + "/pheno");
        VariantMetadataManager metadataManager = new VariantMetadataManager();
        metadataManager.load(metaFilename);
        Pedigree pedigree = metadataManager.getPedigree(datasetName);
        new PedigreeManager().save(pedigree, phenoFile.toPath());

        Map<String, String> pedigreeMap = new LinkedHashMap<>();
        for (String key : pedigree.getIndividuals().keySet()) {
            pedigreeMap.put(key, pedigree.getIndividuals().get(key).getId());
        }

        String sql = "SELECT * FROM vcf";
        Dataset<Row> rows = vd.sqlContext().sql(sql);

        List<String> regions = new ArrayList<>();
        regions.add("22:40766594-40806293");
        regions.add("22:32755892-32767063");
        regions.add("22:32755894-32766972");
        regions.add("22:29834571-29838444");
        regions.add("22:31835344-31885547");
        regions.add("22:31608249-31676066");
        regions.add("22:38615297-38668670");
        regions.add("22:38822332-38851203");
        regions.add("22:20455993-20461786");

        JavaPairRDD<String, Iterable<Row>> groups = rows.javaRDD().groupBy(row -> {
            String key = null;
            for (String region: regions) {
                String[] fields = region.split("[:-]");
                if (fields[0].equals(row.getString(2))
                        && Integer.parseInt(fields[1]) <= row.getInt(4)
                        && Integer.parseInt(fields[2]) >= row.getInt(3)) {
                    key = region;
                    break;
                }
            }
            return key;
        });

        JavaRDD<String> results = groups.map(new Function<Tuple2<String, Iterable<Row>>, String>() {
            @Override
            public String call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                StringBuilder sb = new StringBuilder();
                BufferedWriter writer;
                Iterator<Row> iterator = tuple._2.iterator();
//                System.out.println("+++++++++++++ " + tuple._1);

                String id = tuple._1.replace(":", "_").replace("-", "_");

                // create temporary files for --inVcf and --setFile
                File setFile = new File(tmpDir.getAbsolutePath() + "/setFile." + id);
                writer = FileUtils.newBufferedWriter(setFile.toPath());
                writer.write(id + "\t" + tuple._1 + "\n");
                writer.close();

                File vcfFile = new File(tmpDir.getAbsolutePath() + "/variants."
                        +  id + ".vcf");
                writer = FileUtils.newBufferedWriter(vcfFile.toPath());
                // header lines
                writer.write("##fileformat=VCFv4.2\n");
                writer.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT");
                // sample names
                for (String key : pedigreeMap.keySet()) {
                    writer.write("\t");
                    writer.write(pedigreeMap.get(key));
                }
                writer.write("\n");
                while (iterator.hasNext()) {
                    Row row = iterator.next();
//                    System.out.println("=================== " + row);

                    // variant lines
                    List<Row> studies = row.getList(12);
                    Row study = null;
                    for (int j = 0; j < studies.size(); j++) {
                        if (studies.get(j).get(0).equals(datasetName)) {
                            study = studies.get(j);
                            break;
                        }
                    }

                    if (study == null) {
                        throw new Exception("Dataset '" + datasetName + "' not found!");
                    }
                    List<Row> files = study.getList(1);
                    String filter = (String) files.get(0).getJavaMap(2).get("FILTER");
                    String qual = (String) files.get(0).getJavaMap(2).get("QUAL");

                    sb.setLength(0);
                    sb.append(row.get(2)).append("\t").append(row.get(3)).append("\t")
                            .append(row.get(0) == null ? "." : row.get(0)).append("\t")
                            .append(row.get(5)).append("\t").append(row.get(6)).append("\t").append(qual).append("\t")
                            .append(filter).append("\t").append(".").append("\t").append(study.getList(3).get(0));

                    for (int j = 0; j < pedigreeMap.size(); j++) {
                        sb.append("\t").append(((WrappedArray) study.getList(4).get(j)).head());
                    }
                    sb.append("\n");
                    writer.write(sb.toString());
                    System.out.println(sb.toString());
                }
                writer.close();

                // compress vcf to bgz
                sb.setLength(0);
                sb.append(props.getProperty("bgzip")).append(" ").append(vcfFile.getAbsolutePath());
                execute(sb.toString(), "Compressing vcf to gz: " + sb);

                // and create tabix index
                sb.setLength(0);
                sb.append(props.getProperty("tabix")).append(" -p vcf ").append(vcfFile.getAbsolutePath()).append(".gz");
                execute(sb.toString(), "Creating tabix index: " + sb);

                // rvtests command line
                sb.setLength(0);
                sb.append(props.getProperty("rvtests"))
                        .append(kernel == null ? " " : " --kernel " + kernel)
                        .append(" --pheno ").append(phenoFile.getAbsolutePath())
                        .append(" --inVcf ").append(vcfFile.getAbsolutePath()).append(".gz")
                        .append(" --setFile ").append(setFile.getAbsolutePath())
                        .append(" --out ").append(tmpDir.getAbsolutePath()).append("/out.").append(id);
                execute(sb.toString(), "Running rvtests: " + sb);

                sb.setLength(0);
                File file = new File(tmpDir.getAbsolutePath() + "/out." + id + ".Skat.assoc");
                if (file.exists()) {
                    BufferedReader reader = FileUtils.newBufferedReader(file.toPath());
                    String line = reader.readLine();
                    while ((line = reader.readLine()) != null) {
                        sb.append(line).append("\n");
                    }
                    reader.close();
                }

                return sb.toString();
            }
        });

        System.out.println(results.collect());
    }

    private Process execute(String cmdline, String label) {
        Process p = null;
        try {
            System.out.println(label);
            System.out.println("Executing: " + cmdline);
            p = Runtime.getRuntime().exec(cmdline);

            System.out.println("\tSTDOUT:");
            System.out.println(readInputStream(p.getInputStream()));
            System.out.println("\tSTDERR:");
            System.out.println(readInputStream(p.getErrorStream()));
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

    @Override
    public void execute() throws AnalysisExecutorException {
    }
}
