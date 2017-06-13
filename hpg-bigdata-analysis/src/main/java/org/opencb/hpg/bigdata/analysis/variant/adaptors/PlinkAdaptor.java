package org.opencb.hpg.bigdata.analysis.variant.adaptors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.opencb.biodata.models.core.pedigree.Individual;
import org.opencb.biodata.models.core.pedigree.Pedigree;
import org.opencb.biodata.models.variant.VariantMetadataManager;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.hpg.bigdata.analysis.AnalysisExecutor;
import org.opencb.hpg.bigdata.analysis.AnalysisExecutorException;
import org.opencb.hpg.bigdata.analysis.exceptions.AnalysisToolException;
import org.opencb.hpg.bigdata.analysis.tools.Status;
import org.opencb.hpg.bigdata.analysis.tools.ToolManager;
import org.opencb.hpg.bigdata.analysis.variant.VariantAnalysisExecutor;
import org.opencb.hpg.bigdata.analysis.variant.VariantFilterOptions;
import org.opencb.hpg.bigdata.core.lib.SparkConfCreator;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jtarraga on 09/06/17.
 */
public class PlinkAdaptor extends VariantAnalysisExecutor {
    private String inFilename;
    private String metaFilename;
    private String outDirname;

    private int splitSize;
    private List<String> plinkParams;
    private VariantFilterOptions filterOptions;

    private static Path toolPath = Paths.get("/tmp/tools");
    private static Path bin = Paths.get(toolPath + "/plink/plink");

    private static String sep = " ";


    public PlinkAdaptor(String inFilename, String metaFilename, String outDirname) {
        this.inFilename = inFilename;
        this.metaFilename = inFilename + AnalysisExecutor.metadataExtension;
        this.outDirname = outDirname;
    }

    @Override
    public void execute() throws AnalysisExecutorException {
        System.out.println("plink params = " + plinkParams);
        System.out.println("filter options = " + filterOptions);

        System.out.println("input filename = " + inFilename);
        List<Row> rows = null;

        try {
            if (sparkSession == null) {
                SparkConf sparkConf = SparkConfCreator.getConf("variant plink", "local", 1, true);
                logger.debug("sparkConf = {}", sparkConf.toDebugString());

                sparkSession = new SparkSession(new SparkContext(sparkConf));
            }
            //VariantDataset variantDataset = filter(Paths.get(inFilename), filterOptions);
            rows = getRows(Paths.get(inFilename), filterOptions);
        } catch (Exception e) {
            logger.error("Error getting variant dataset " + inFilename);
            logger.error(e.getMessage());
        }

        // prepare input files for PLINK
        String mapFilename = outDirname + "/out.map";
        String pedFilename = outDirname + "/out.ped";
        prepareMapPedFiles(rows, mapFilename, pedFilename);

        Path out = Paths.get(outDirname + "/out");

        ToolManager toolManager;
        String commandLine = createCommandLine();
        // add input and output parameters to the PLINK command line
        commandLine += (" --file " + out + " --out " + out);
        try {
            toolManager = new ToolManager(toolPath);
            System.out.println(commandLine);
            toolManager.runCommandLine(commandLine, Paths.get(outDirname));
        } catch (AnalysisToolException e) {
            e.printStackTrace();
        }

        try {
            ObjectReader reader = new ObjectMapper().reader(Status.class);
            Status status = reader.readValue(new File(outDirname + "/status.json"));
            if (!Status.DONE.equals(status.getName())) {
                logger.error("Something wrong happended executing " + commandLine);
            }
            //assertEquals(Status.DONE, status.getName());

            //assertTrue(tmp.resolve("test.bam.bai").toFile().exists());
        } catch (Exception e) {

            e.printStackTrace();
        }
    }

    /**
     * Create PLINK command line from input parameters.
     *
     * @return  Command line
     */
    private String createCommandLine() {
        StringBuilder sb = new StringBuilder(bin.toString());

        for (String param: plinkParams) {
            String[] split = param.split("=");
            if (split[0] != null) {
                sb.append(" --").append(split[0]);
                if (split.length > 1) {
                    sb.append(" ").append(split[1]);
                }
            } else {
                logger.error("Discarding invalid PLINK parameter: " + param);
            }
        }
        return sb.toString();
    }

    /**
     * Prepare PLINK input files (map and ped files).
     *
     * @param rows          Rows containing the target variant
     * @param mapFilename   Map file name
     * @param pedFilename   Pedigree file name
     */
    private void prepareMapPedFiles(List<Row> rows, String mapFilename, String pedFilename) {

        // sanity check
        if (rows == null || rows.size() == 0) {
            logger.warn("No variants found!");
            return;
        }

        // get pedigree
        VariantMetadataManager variantMetadataManager = new VariantMetadataManager();
        Pedigree pedigree = null;
        try {
            variantMetadataManager.load(metaFilename);
            pedigree = variantMetadataManager.getPedigree(datasetName);
        } catch (IOException e) {
            logger.error("Error writing PED file: " + e.getMessage());
        }

        // get sample IDs
        // TODO: improve it taking into account dataset names, files...
        VariantMetadata variantMetadata = variantMetadataManager.getVariantMetadata();
        List<String> sampleIds = variantMetadata.getDatasets().get(0).getFiles().get(0).getSampleIds();
        int numSamples = sampleIds.size();
        Map<String, List<String>> sampleMap = new HashMap<>(numSamples);
        for (int i = 0; i < numSamples; i++) {
            sampleMap.put(sampleIds.get(i), new ArrayList<>());
        }

        // map file
        System.out.println("out dir = " + outDirname);
        PrintWriter mapWriter = null;
        PrintWriter pedWriter = null;
        try {
            mapWriter = new PrintWriter(mapFilename);
            pedWriter = new PrintWriter(pedFilename);
            // for each variant
            for (Row row: rows) {
                System.out.println("id = " + row.get(row.fieldIndex("id")) + ", chrom = "
                        + row.get(row.fieldIndex("chromosome")) + ", start = " + row.get(row.fieldIndex("start")));
                mapWriter.write(row.get(row.fieldIndex("chromosome")) + sep
                        + row.get(row.fieldIndex("id")) + sep
                        + "0" + sep
                        + row.get(row.fieldIndex("start"))
                        + "\n");

                // for each sample
                for (int i = 0; i < numSamples; i++) {
                    Object gt = JavaConversions.mutableSeqAsJavaList((WrappedArray) row.getList(3).get(i)).get(0);
                    System.out.println("\tgt = " + gt);
                    sampleMap.get(sampleIds.get(i)).add((String) gt);
                }
            }

            StringBuilder sb = new StringBuilder();
            for (String key: pedigree.getIndividuals().keySet()) {
                Individual individual = pedigree.getIndividuals().get(key);
                String[] split = key.split("_");
                String familiId = split[0];
                String individualId = split[1];

                sb.setLength(0);
                sb.append(familiId).append(sep).append(individualId).append(sep);
                sb.append(individual.getFather() == null ? "0" : individual.getFather().getId()).append(sep);
                sb.append(individual.getMother() == null ? "0" : individual.getMother().getId()).append(sep);
                sb.append(individual.getSex().getValue()).append(sep);
                sb.append(individual.getPhenotype().getValue()).append(sep);
                for (String gt: sampleMap.get(individualId)) {
                    sb.append(gt.replaceAll("[\\|]", sep).replaceAll("1", "2").replaceAll("0", "1")).append(sep);
                }
                sb.append("\n");
                pedWriter.write(sb.toString());
                System.out.println("key = " + key);
                System.out.println("\tindividual id = " + individual.getId() + ", family id = " + individual.getFamily());
            }


            mapWriter.close();
            pedWriter.close();
        } catch (FileNotFoundException e) {
            logger.error("Error writing MAP file: " + e.getMessage());
        }

        // ped file
            //variantMetadataManager.load(metaFilename);
            //PedigreeManager pedigreeManager = new PedigreeManager();
            //pedigreeManager.save(variantMetadataManager.getPedigree(datasetName), Paths.get(pedFilename));
    }

    public String getInFilename() {
        return inFilename;
    }

    public void setInFilename(String inFilename) {
        this.inFilename = inFilename;
    }

    public String getMetaFilename() {
        return metaFilename;
    }

    public void setMetaFilename(String metaFilename) {
        this.metaFilename = metaFilename;
    }

    public String getOutDirname() {
        return outDirname;
    }

    public void setOutDirname(String outDirname) {
        this.outDirname = outDirname;
    }

    public int getSplitSize() {
        return splitSize;
    }

    public void setSplitSize(int splitSize) {
        this.splitSize = splitSize;
    }

    public List<String> getPlinkParams() {
        return plinkParams;
    }

    public void setPlinkParams(List<String> plinkParams) {
        this.plinkParams = plinkParams;
    }

    public VariantFilterOptions getFilterOptions() {
        return filterOptions;
    }

    public void setFilterOptions(VariantFilterOptions filterOptions) {
        this.filterOptions = filterOptions;
    }
}
