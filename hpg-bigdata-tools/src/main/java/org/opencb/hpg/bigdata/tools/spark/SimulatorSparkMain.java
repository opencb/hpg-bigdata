package org.opencb.hpg.bigdata.tools.spark;

import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.variant.simulator.VariantSimulatorConfiguration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by pawan on 08/02/16.
 */
public class SimulatorSparkMain {

    private static VariantSimulatorConfiguration variantSimulatorConfiguration;


    public SimulatorSparkMain() {
        variantSimulatorConfiguration = new VariantSimulatorConfiguration();
    }

    public static void main(String[] args) throws FileNotFoundException {

        SimulatorSpark simulatorSpark = new SimulatorSpark();

        System.out.println("args.length = " + args.length);
        System.out.println("args[0] = " + args[0]);
        System.out.println("args[1] = " + args[1]);
        System.out.println("args[2] = " + args[2]);
        System.out.println("args[3] = " + args[3]);
        System.out.println("args[4] = " + args[4]);

//        if (args.length < 5) {
//            System.out.println("Usage : " + "<input file path>, <output file path>, <number of variants>, <chunk size>, <region>");
//        }

        String inputFile = args[0];
        String outputFile = args[1];
        String chunk = args[2];
        String numOfVariants = args[3];
        String numOfSamples = args[4];
        String region = "1:1002303-1101234";

        String sparkInputPath = new SimulatorSparkMain().createInputFile(inputFile, numOfVariants, chunk, region);
        System.out.println("inPath = " + sparkInputPath);

//        simulatorSpark.inputRDD(sparkInputPath, outputFile);
//        simulatorSpark.simulate(Integer.parseInt(numOfVariants), defaultNumSamples, getRegionList(region));

        simulatorSpark.variantSimulatorInputs(sparkInputPath, outputFile, Integer.parseInt(numOfVariants),
                Integer.parseInt(numOfSamples), getRegionList(region));
    }


    private String createInputFile(String inputPath, String numOfVariants, String chunk, String region)
            throws FileNotFoundException {

        //new code to generate inout file for spark

        int numVariants;
        int chunkSize;
        File tmpFile;

        List<Region> regionList = getRegionList(region);

        //Calculate number of variants per chunk
        long totalGenomeSize = 0;
        for (Region region1 : regionList) {
            totalGenomeSize += region1.getEnd();
        }
        numVariants = Integer.parseInt(numOfVariants);
        chunkSize =  Integer.parseInt(chunk);

        int numVariantsPerChunk = Math.round(numVariants / (totalGenomeSize / chunkSize));

        // create the input file for Hadoop MR
        String fileName = "spark_input.txt";
//        File tmpFile = new File("/tmp/" + fileName);


        if (inputPath.endsWith("/")) {
            tmpFile = new File(inputPath + fileName);
        } else {
            tmpFile = new File(inputPath + "/" + fileName);
        }


        //Check if file exist. Delete if exist
        if (tmpFile.exists() && !tmpFile.isDirectory()) {
            System.out.println("File " + tmpFile + " already exist. Deleting file...");
            tmpFile.delete();
            System.out.println("File " + tmpFile + " deleted");
        }

        //Write chromosome, start, end and number of variants per chunk
        PrintWriter printWriter = new PrintWriter(tmpFile);
        for (Region region2 : regionList) {
            int start = 1;
            int end = chunkSize;
            int numChunks = region2.getEnd() / chunkSize;
            for (int i = 0; i < numChunks; i++) {
                printWriter.println(region2.getChromosome() + "\t" + String.format("%09d", start) + "\t"
                        + String.format("%09d", end) + "\t" + numVariantsPerChunk);
                start += chunkSize;
                end += chunkSize;
            }
        }
        printWriter.close();

        String inputFileSpark;
        if (tmpFile.toString().startsWith("/")) {
            inputFileSpark = "file:" + tmpFile.toString();
        } else {
            inputFileSpark = "file:/" + tmpFile.toString();
        }

        return inputFileSpark;
    }


    private static List<Region> getRegionList(String region) {

        List<Region> getRegions;
        //Set region value if null or if not null take default
        String regionData = region.replace("[", "").replace("]", "");
        if (regionData.isEmpty() || regionData.equals("") || regionData.equals("1:1002303-1101234")) {
            System.out.println("regionData = " + regionData.length());
            // new VariantSimulatorConfiguration().getRegions();
            getRegions = variantSimulatorConfiguration.getRegions();
        } else {
            getRegions = new ArrayList<>();
            String[] regionArray = regionData.split(",");
            for (int i = 0; i <= regionArray.length - 1; i++) {
                System.out.println("Entered regions are : " + regionArray[i]);
                getRegions.add(Region.parseRegion(regionArray[i].trim()));
            }
        }

        return getRegions;
    }

}
