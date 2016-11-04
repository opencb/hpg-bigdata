/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.hpg.bigdata.app.cli.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStatsCalculator;
import org.opencb.biodata.tools.alignment.stats.AvroAlignmentGlobalStatsCalculator;
import org.opencb.commons.utils.FileUtils;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.core.avro.AlignmentAvroSerializer;
import org.opencb.hpg.bigdata.core.lib.AlignmentDataset;
import org.opencb.hpg.bigdata.core.lib.SparkConfCreator;
import org.opencb.hpg.bigdata.core.parquet.AlignmentParquetConverter;
import org.opencb.hpg.bigdata.core.utils.ReadAlignmentUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

/**
 * Created by imedina on 16/03/15.
 */
public class AlignmentCommandExecutor extends CommandExecutor {

    private LocalCliOptionsParser.AlignmentCommandOptions alignmentCommandOptions;

    public static final String BAM_HEADER_SUFFIX = ".header";

    public AlignmentCommandExecutor(LocalCliOptionsParser.AlignmentCommandOptions alignmentCommandOptions) {
        this.alignmentCommandOptions = alignmentCommandOptions;
    }

    /*
     * Parse specific 'alignment' command options
     */
    public void execute() throws Exception {
        String subCommand = alignmentCommandOptions.getParsedSubCommand();

        switch (subCommand) {
            case "convert":
                init(alignmentCommandOptions.convertAlignmentCommandOptions.commonOptions.logLevel,
                        alignmentCommandOptions.convertAlignmentCommandOptions.commonOptions.verbose,
                        alignmentCommandOptions.convertAlignmentCommandOptions.commonOptions.conf);
                convert();
                break;
            case "sort":
                init(alignmentCommandOptions.sortAlignmentCommandOptions.commonOptions.logLevel,
                        alignmentCommandOptions.sortAlignmentCommandOptions.commonOptions.verbose,
                        alignmentCommandOptions.sortAlignmentCommandOptions.commonOptions.conf);
                sort();
                break;
            case "stats":
                init(alignmentCommandOptions.statsAlignmentCommandOptions.commonOptions.logLevel,
                        alignmentCommandOptions.statsAlignmentCommandOptions.commonOptions.verbose,
                        alignmentCommandOptions.statsAlignmentCommandOptions.commonOptions.conf);
                stats();
                break;
            case "depth":
                init(alignmentCommandOptions.depthAlignmentCommandOptions.commonOptions.logLevel,
                        alignmentCommandOptions.depthAlignmentCommandOptions.commonOptions.verbose,
                        alignmentCommandOptions.depthAlignmentCommandOptions.commonOptions.conf);
                depth();
                break;
            case "query":
                init(alignmentCommandOptions.queryAlignmentCommandOptions.commonOptions.logLevel,
                        alignmentCommandOptions.queryAlignmentCommandOptions.commonOptions.verbose,
                        alignmentCommandOptions.queryAlignmentCommandOptions.commonOptions.conf);
                query();
                break;
            case "view":
                init(alignmentCommandOptions.queryAlignmentCommandOptions.commonOptions.logLevel,
                        alignmentCommandOptions.queryAlignmentCommandOptions.commonOptions.verbose,
                        alignmentCommandOptions.queryAlignmentCommandOptions.commonOptions.conf);
                view();
                break;
            /*
            case "align":
                System.out.println("Sub-command 'align': Not yet implemented for the command 'alignment' !");
                break;
*/
            default:
                break;
        }
    }

    private void convert() throws IOException {

        // sanity check: paremeter 'to'
        String to = alignmentCommandOptions.convertAlignmentCommandOptions.to.toLowerCase();
        if (!to.equals("avro") && !to.equals("parquet")) {
            throw new IllegalArgumentException("Unknown serialization format: " + to + ". Valid values: avro, parquet");
        }
        boolean toParquet = to.equals("parquet");

        String from = alignmentCommandOptions.convertAlignmentCommandOptions.from.toLowerCase();
        if (!from.equals("bam") && !from.equals("avro")) {
            throw new IllegalArgumentException("Unknown input format: " + from + ". Valid values: bam, avro");
        }
        boolean fromAvro = from.equals("avro");

        // sanity check: parameter 'compression'
        String compressionCodecName = alignmentCommandOptions.convertAlignmentCommandOptions.compression.toLowerCase();
        if (!compressionCodecName.equals("gzip")
                && !compressionCodecName.equals("snappy")) {
            throw new IllegalArgumentException("Unknown compression method: " + compressionCodecName
                    + ". Valid values: gzip, snappy");
        }

        // sanity check: input file
        Path inputPath = Paths.get(alignmentCommandOptions.convertAlignmentCommandOptions.input);
        FileUtils.checkFile(inputPath);

        // sanity check: output file
        String output = CliUtils.getOutputFilename(alignmentCommandOptions.convertAlignmentCommandOptions.input,
                alignmentCommandOptions.convertAlignmentCommandOptions.output, to);

        long startTime, elapsedTime;
        boolean binQualities = alignmentCommandOptions.convertAlignmentCommandOptions.binQualities;

        // convert to parquet if required
        if (toParquet) {
            // sanity check: rowGroupSize and pageSize for parquet conversion
            int rowGroupSize = alignmentCommandOptions.convertAlignmentCommandOptions.blockSize;
            if (rowGroupSize <= 0) {
                throw new IllegalArgumentException("Invalid block size: " + rowGroupSize
                        + ". It must be greater than 0");
            }
            int pageSize = alignmentCommandOptions.convertAlignmentCommandOptions.pageSize;
            if (pageSize <= 0) {
                throw new IllegalArgumentException("Invalid page size: " + pageSize
                        + ". It must be greater than 0");
            }

            // create the Parquet writer and add the necessary filters
            AlignmentParquetConverter parquetConverter = new AlignmentParquetConverter(
                    CompressionCodecName.fromConf(compressionCodecName), binQualities, rowGroupSize, pageSize);

            // set minimum mapping quality filter
            if (alignmentCommandOptions.convertAlignmentCommandOptions.minMapQ > 0) {
                parquetConverter.addMinMapQFilter(alignmentCommandOptions.convertAlignmentCommandOptions.minMapQ);
            }

            // region filter management,
            // we use the same region list to store all regions from both parameter --regions and
            // parameter --region-file
            List<Region> regions = CliUtils.getRegionList(alignmentCommandOptions.convertAlignmentCommandOptions.regions,
                    alignmentCommandOptions.convertAlignmentCommandOptions.regionFilename);
            if (regions != null && regions.size() > 0) {
                parquetConverter.addRegionFilter(regions, false);
            }

            if (fromAvro) {
                // convert to AVRO -> PARQUET
                InputStream inputStream = new FileInputStream(inputPath.toString());
                System.out.println("\n\nStarting AVRO->PARQUET conversion...\n");
                startTime = System.currentTimeMillis();
                parquetConverter.toParquetFromAvro(inputStream, output);
                elapsedTime = System.currentTimeMillis() - startTime;
                System.out.println("\n\nFinish AVRO->PARQUET conversion in " + (elapsedTime / 1000F) + " sec\n");

                // header file management
                File headerFile = new File(inputPath.toString() + ".header");
                if (headerFile.exists()) {
                    File outHeaderFile = new File(output + ".header");
                    Files.copy(headerFile.toPath(), new FileOutputStream(outHeaderFile));
                }
            } else {
                // convert to BAM -> PARQUET
                System.out.println("\n\nStarting BAM->PARQUET conversion...\n");
                startTime = System.currentTimeMillis();
                parquetConverter.toParquetFromBam(inputPath.toString(), output);
                elapsedTime = System.currentTimeMillis() - startTime;
                System.out.println("\n\nFinish BAM->PARQUET conversion in " + (elapsedTime / 1000F) + " sec\n");
            }
        } else {
            // convert to BAM -> AVRO

            AlignmentAvroSerializer avroSerializer;
            avroSerializer = new AlignmentAvroSerializer(compressionCodecName, binQualities);

            // set minimum mapping quality filter
            if (alignmentCommandOptions.convertAlignmentCommandOptions.minMapQ > 0) {
                avroSerializer.addMinMapQFilter(alignmentCommandOptions.convertAlignmentCommandOptions.minMapQ);
            }

            // region filter management,
            // we use the same region list to store all regions from both parameter --regions and
            // parameter --region-file
            List<Region> regions = CliUtils.getRegionList(alignmentCommandOptions.convertAlignmentCommandOptions.regions,
                    alignmentCommandOptions.convertAlignmentCommandOptions.regionFilename);
            if (regions != null && regions.size() > 0) {
                avroSerializer.addRegionFilter(regions, false);
            }

            System.out.println("\n\nStarting BAM->AVRO conversion...\n");
            startTime = System.currentTimeMillis();
            avroSerializer.toAvro(inputPath.toString(), output);
            elapsedTime = System.currentTimeMillis() - startTime;
            System.out.println("\n\nFinish BAM->AVRO conversion in " + (elapsedTime / 1000F) + " sec\n");
        }



//        if (alignmentCommandOptions.convertAlignmentCommandOptions.toBam) {
//            // conversion: GA4GH/Avro model -> BAM
//
//            // header management: read it from a separate file
//            File file = new File(input + BAM_HEADER_SUFFIX);
//            FileInputStream fis = new FileInputStream(file);
//            byte[] data = new byte[(int) file.length()];
//            fis.read(data);
//            fis.close();
//
//            InputStream is = new FileInputStream(input);
//
//            String textHeader = new String(data);
//
//            LineReader lineReader = new StringLineReader(textHeader);
//            SAMFileHeader header = new SAMTextHeaderCodec().decode(lineReader, textHeader);
//
//            // reader
//            DataFileStream<ReadAlignment> reader = new DataFileStream<ReadAlignment>(is, new SpecificDatumReader<>(ReadAlignment.class));
//
//            // writer
//            OutputStream os = new FileOutputStream(new File(output));
//            SAMFileWriter writer = new SAMFileWriterFactory().makeBAMWriter(header, false, new File(output));
//
//            // main loop
//            int reads = 0;
//            SAMRecord samRecord;
//            SAMRecord2ReadAlignmentConverter converter = new SAMRecord2ReadAlignmentConverter();
//            for (ReadAlignment readAlignment : reader) {
//                samRecord = converter.backward(readAlignment);
//                samRecord.setHeader(header);
//                writer.addAlignment(samRecord);
//                if (++reads % 100_000 == 0) {
//                    System.out.println("Converted " + reads + " reads");
//                }
//            }
//
//            // close
//            reader.close();
//            writer.close();
//            os.close();
//            is.close();
//
//        } else {
//
//            // conversion: BAM -> GA4GH/Avro model
///*            System.out.println("Loading library hpgbigdata...");
//            System.out.println("\tjava.libary.path = " + System.getProperty("java.library.path"));
//            System.loadLibrary("hpgbigdata");
//            System.out.println("...done!");
//            new NativeSupport().bam2ga(input, output, compressionCodecName == null
//                    ? "snappy"
//                    : compressionCodecName, alignmentCommandOptions.convertAlignmentCommandOptions.adjustQuality);
//
//            try {
//                // header management: saved it in a separate file
//                SamReader reader = SamReaderFactory.makeDefault().open(new File(input));
//                SAMFileHeader header = reader.getFileHeader();
//                PrintWriter pwriter = null;
//                pwriter = new PrintWriter(new FileWriter(output + BAM_HEADER_SUFFIX));
//                pwriter.write(header.getTextHeader());
//                pwriter.close();
//            } catch (IOException e) {
//                throw e;
//            }
//*/
//
//            boolean adjustQuality = alignmentCommandOptions.convertAlignmentCommandOptions.adjustQuality;
//            AlignmentAvroSerializer avroSerializer = new AlignmentAvroSerializer(compressionCodecName);
//            avroSerializer.toAvro(input, output);
//
//        }
    }

    public void sort() throws Exception {
        // check mandatory parameter 'input file'
        Path inputPath = Paths.get(alignmentCommandOptions.sortAlignmentCommandOptions.input);
        FileUtils.checkFile(inputPath);

        // TODO: to take the spark home from somewhere else
        SparkConf sparkConf = SparkConfCreator.getConf("variant query", "local", 1,
                true, "/home/jtarraga/soft/spark-2.0.0/");
        System.out.println("sparkConf = " + sparkConf.toDebugString());
        SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));

//        SparkConf sparkConf = SparkConfCreator.getConf("MyTest", "local", 1, true, "/home/jtarraga/soft/spark-2.0.0/");
//        SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));

        AlignmentDataset ad = new AlignmentDataset();

        ad.load(alignmentCommandOptions.sortAlignmentCommandOptions.input, sparkSession);

        // sort
        ad.orderBy("alignment.position.referenceName", "alignment.position.position");

        // save the dataset
        logger.warn("The current query implementation saves the resulting dataset in Avro format.");
        CliUtils.saveDatasetAsOneFile(ad, "avro", alignmentCommandOptions.sortAlignmentCommandOptions.output, logger);
    }

    private void stats() throws IOException {
        // get input parameters
        String input = alignmentCommandOptions.statsAlignmentCommandOptions.input;
        String output = alignmentCommandOptions.statsAlignmentCommandOptions.output;

        try {
            // reader
            InputStream is = new FileInputStream(input);
            DataFileStream<ReadAlignment> reader = new DataFileStream<>(is, new SpecificDatumReader<>(ReadAlignment.class));

            AlignmentGlobalStats stats;
            AlignmentGlobalStats totalStats = new AlignmentGlobalStats();
            AlignmentGlobalStatsCalculator calculator = new AvroAlignmentGlobalStatsCalculator();

            // main loop
            for (ReadAlignment readAlignment : reader) {
                stats = calculator.compute(readAlignment);
                calculator.update(stats, totalStats);
            }

            // close reader
            reader.close();
            is.close();

            // write results
            PrintWriter writer = new PrintWriter(new File(output + "/stats.json"));
            writer.write(totalStats.toJSON());
            writer.close();

        } catch (Exception e) {
            throw e;
        }
    }

    private void depth() throws IOException {
        // get input parameters
        String input = alignmentCommandOptions.depthAlignmentCommandOptions.input;
        String output = alignmentCommandOptions.depthAlignmentCommandOptions.output;

        HashMap<String, Integer> regionLength = new HashMap<>();

        //TODO: fix using the new RegionCoverage, JT
//        try {
//            // header management
//            BufferedReader br = new BufferedReader(new FileReader(input + BAM_HEADER_SUFFIX));
//            String line, fieldName, fieldLength;
//            String[] fields;
//            String[] subfields;
//            while ((line = br.readLine()) != null) {
//                if (line.startsWith("@SQ")) {
//                    fields = line.split("\t");
//                    subfields = fields[1].split(":");
//                    fieldName = subfields[1];
//                    subfields = fields[2].split(":");
//                    fieldLength = subfields[1];
//
//                    regionLength.put(fieldName, Integer.parseInt(fieldLength));
//                }
//            }
//            br.close();
//
//            // reader
//            InputStream is = new FileInputStream(input);
//            DataFileStream<ReadAlignment> reader = new DataFileStream<>(is, new SpecificDatumReader<>(ReadAlignment.class));
//
//            // writer
//            PrintWriter writer = new PrintWriter(new File(output + "/depth.txt"));
//
//            String chromName = "";
//            int[] chromDepth;
//
//            RegionDepth regionDepth;
//            RegionDepthCalculator calculator = new RegionDepthCalculator();
//
//            int pos;
//            long prevPos = 0L;
//
//            // main loop
//            chromDepth = null;
//            for (ReadAlignment readAlignment : reader) {
//                if (readAlignment.getAlignment() != null) {
//                    regionDepth = calculator.compute(readAlignment);
//                    if (chromDepth == null) {
//                        chromName = regionDepth.chrom;
//                        chromDepth = new int[regionLength.get(regionDepth.chrom)];
//                    }
//                    if (!chromName.equals(regionDepth.chrom)) {
//                        // write depth
//                        int length = chromDepth.length;
//                        for (int i = 0; i < length; i++) {
//                            writer.write(chromName + "\t" + (i + 1) + "\t" + chromDepth[i] + "\n");
//                        }
//
//                        // init
//                        prevPos = 0L;
//                        chromName = regionDepth.chrom;
//                        chromDepth = new int[regionLength.get(regionDepth.chrom)];
//                    }
//                    if (prevPos > regionDepth.position) {
//                        throw new IOException("Error: the input file (" + input + ") is not sorted (reads out of order).");
//                    }
//
//                    pos = (int) regionDepth.position;
//                    for (int i: regionDepth.array) {
//                        chromDepth[pos] += regionDepth.array[i];
//                        pos++;
//                    }
//                    prevPos = regionDepth.position;
//                }
//            }
//            // write depth
//            int length = chromDepth.length;
//            for (int i = 0; i < length; i++) {
//                if (chromDepth[i] > 0) {
//                    writer.write(chromName + "\t" + (i + 1) + "\t" + chromDepth[i] + "\n");
//                }
//            }
//
//            // close
//            reader.close();
//            is.close();
//            writer.close();
//        } catch (Exception e) {
//            throw e;
//        }
    }

    public void query() throws Exception {
        // check mandatory parameter 'input file'
        Path inputPath = Paths.get(alignmentCommandOptions.queryAlignmentCommandOptions.input);
        FileUtils.checkFile(inputPath);

        // TODO: to take the spark home from somewhere else
        SparkConf sparkConf = SparkConfCreator.getConf("variant query", "local", 1,
                true, "/home/jtarraga/soft/spark-2.0.0/");
        System.out.println("sparkConf = " + sparkConf.toDebugString());
        SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));

//        SparkConf sparkConf = SparkConfCreator.getConf("MyTest", "local", 1, true, "/home/jtarraga/soft/spark-2.0.0/");
//        SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));

        AlignmentDataset ad = new AlignmentDataset();

        ad.load(alignmentCommandOptions.queryAlignmentCommandOptions.input, sparkSession);
        ad.createOrReplaceTempView("alignment");

        // query for region
        List<Region> regions = CliUtils.getRegionList(alignmentCommandOptions.queryAlignmentCommandOptions.regions,
                alignmentCommandOptions.queryAlignmentCommandOptions.regionFile);
        if (regions != null && regions.size() > 0) {
            ad.regionFilter(regions);
        }

        // query for minimun mapping quality
        if (alignmentCommandOptions.queryAlignmentCommandOptions.minMapQ > 0) {
            ad.mappingQualityFilter(">=" + alignmentCommandOptions.queryAlignmentCommandOptions.minMapQ);
        }

        // query for flags
        if (alignmentCommandOptions.queryAlignmentCommandOptions.requireFlags != Integer.MAX_VALUE)  {
            ad.flagFilter("" + alignmentCommandOptions.queryAlignmentCommandOptions.requireFlags, false);
        }
        if (alignmentCommandOptions.queryAlignmentCommandOptions.filteringFlags != 0) {
            ad.flagFilter("" + alignmentCommandOptions.queryAlignmentCommandOptions.filteringFlags, true);
        }

        // query for template length
        if (alignmentCommandOptions.queryAlignmentCommandOptions.minTLen != 0) {
            ad.templateLengthFilter(">=" + alignmentCommandOptions.queryAlignmentCommandOptions.minTLen);
        }
        if (alignmentCommandOptions.queryAlignmentCommandOptions.maxTLen != Integer.MAX_VALUE) {
            ad.templateLengthFilter("<=" + alignmentCommandOptions.queryAlignmentCommandOptions.maxTLen);
        }

        // query for alignment length
        if (alignmentCommandOptions.queryAlignmentCommandOptions.minALen != 0) {
            ad.alignmentLengthFilter(">=" + alignmentCommandOptions.queryAlignmentCommandOptions.minALen);
        }
        if (alignmentCommandOptions.queryAlignmentCommandOptions.maxALen != Integer.MAX_VALUE) {
            ad.alignmentLengthFilter("<=" + alignmentCommandOptions.queryAlignmentCommandOptions.maxALen);
        }

        // apply previous filters
        ad.update();

        // save the dataset
        logger.warn("The current query implementation saves the resulting dataset in Avro format.");
        CliUtils.saveDatasetAsOneFile(ad, "avro", alignmentCommandOptions.queryAlignmentCommandOptions.output, logger);
    }

    public void view() throws Exception {
        Path input = Paths.get(alignmentCommandOptions.viewAlignmentCommandOptions.input);
        int head = alignmentCommandOptions.viewAlignmentCommandOptions.head;

        // open
        InputStream is = new FileInputStream(input.toFile());
        DataFileStream<ReadAlignment> reader = new DataFileStream<>(is,
                new SpecificDatumReader<>(ReadAlignment.class));

        long counter = 0;
        ObjectMapper mapper = new ObjectMapper();
        if (alignmentCommandOptions.viewAlignmentCommandOptions.sam) {
            // sam
            // first, header
            File headerFile = new File(input.toString() + ".header");
            if (headerFile.exists()) {
                System.out.println(org.apache.commons.io.FileUtils.readFileToString(headerFile).trim());
            }

            // and then, alignments
            for (ReadAlignment alignment : reader) {
                System.out.println(ReadAlignmentUtils.getSamString(alignment));
                counter++;
                if (head > 0 && counter == head) {
                    break;
                }
            }
        } else if (alignmentCommandOptions.viewAlignmentCommandOptions.schema) {
            // schema
            System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(
                    mapper.readValue(reader.getSchema().toString(), Object.class)));
        } else {
            // main
            System.out.println("[");
            for (ReadAlignment alignment : reader) {
                // remove nucleotide sequences ?
                if (alignmentCommandOptions.viewAlignmentCommandOptions.excludeSequences) {
                    alignment.setAlignedSequence(null);
                }

                // remove quality sequences ?
                if (alignmentCommandOptions.viewAlignmentCommandOptions.excludeQualities) {
                    alignment.setAlignedQuality(null);
                }

                System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(
                        mapper.readValue(alignment.toString(), Object.class)));
                counter++;
                if (head > 0 && counter == head) {
                    break;
                }
                System.out.println(",");
            }
            System.out.println("]");
        }

        // close
        reader.close();
    }

}
