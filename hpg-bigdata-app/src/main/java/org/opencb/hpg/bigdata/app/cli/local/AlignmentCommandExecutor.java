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

import htsjdk.samtools.*;
import htsjdk.samtools.util.LineReader;
import htsjdk.samtools.util.StringLineReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.AlignmentOptions;
import org.opencb.biodata.tools.alignment.BamManager;
import org.opencb.biodata.tools.alignment.BamUtils;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.commons.utils.FileUtils;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.core.avro.AlignmentAvroSerializer;
import org.opencb.hpg.bigdata.core.converters.SAMRecord2ReadAlignmentConverter;
import org.opencb.hpg.bigdata.core.lib.AlignmentDataset;
import org.opencb.hpg.bigdata.core.lib.SparkConfCreator;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
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
            case "stats":
                init(alignmentCommandOptions.statsAlignmentCommandOptions.commonOptions.logLevel,
                        alignmentCommandOptions.statsAlignmentCommandOptions.commonOptions.verbose,
                        alignmentCommandOptions.statsAlignmentCommandOptions.commonOptions.conf);
                stats();
                break;
            case "coverage":
                init(alignmentCommandOptions.coverageAlignmentCommandOptions.commonOptions.logLevel,
                        alignmentCommandOptions.coverageAlignmentCommandOptions.commonOptions.verbose,
                        alignmentCommandOptions.coverageAlignmentCommandOptions.commonOptions.conf);
                coverage();
                break;
            case "query":
                init(alignmentCommandOptions.queryAlignmentCommandOptions.commonOptions.logLevel,
                        alignmentCommandOptions.queryAlignmentCommandOptions.commonOptions.verbose,
                        alignmentCommandOptions.queryAlignmentCommandOptions.commonOptions.conf);
                query();
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
        String input = alignmentCommandOptions.convertAlignmentCommandOptions.input;
        String output = alignmentCommandOptions.convertAlignmentCommandOptions.output;
        String compressionCodecName = alignmentCommandOptions.convertAlignmentCommandOptions.compression;

        // sanity check
        if (compressionCodecName.equals("null")) {
            compressionCodecName = "deflate";
        }

        if (alignmentCommandOptions.convertAlignmentCommandOptions.toBam) {
            // conversion: GA4GH/Avro model -> BAM

            // header management: read it from a separate file
            File file = new File(input + BAM_HEADER_SUFFIX);
            FileInputStream fis = new FileInputStream(file);
            byte[] data = new byte[(int) file.length()];
            fis.read(data);
            fis.close();

            InputStream is = new FileInputStream(input);

            String textHeader = new String(data);

            LineReader lineReader = new StringLineReader(textHeader);
            SAMFileHeader header = new SAMTextHeaderCodec().decode(lineReader, textHeader);

            // reader
            DataFileStream<ReadAlignment> reader = new DataFileStream<ReadAlignment>(is, new SpecificDatumReader<>(ReadAlignment.class));

            // writer
            OutputStream os = new FileOutputStream(new File(output));
            SAMFileWriter writer = new SAMFileWriterFactory().makeBAMWriter(header, false, new File(output));

            // main loop
            int reads = 0;
            SAMRecord samRecord;
            SAMRecord2ReadAlignmentConverter converter = new SAMRecord2ReadAlignmentConverter();
            for (ReadAlignment readAlignment : reader) {
                samRecord = converter.backward(readAlignment);
                samRecord.setHeader(header);
                writer.addAlignment(samRecord);
                if (++reads % 100_000 == 0) {
                    System.out.println("Converted " + reads + " reads");
                }
            }

            // close
            reader.close();
            writer.close();
            os.close();
            is.close();

        } else {

            // conversion: BAM -> GA4GH/Avro model
/*            System.out.println("Loading library hpgbigdata...");
            System.out.println("\tjava.libary.path = " + System.getProperty("java.library.path"));
            System.loadLibrary("hpgbigdata");
            System.out.println("...done!");
            new NativeSupport().bam2ga(input, output, compressionCodecName == null
                    ? "snappy"
                    : compressionCodecName, alignmentCommandOptions.convertAlignmentCommandOptions.adjustQuality);

            try {
                // header management: saved it in a separate file
                SamReader reader = SamReaderFactory.makeDefault().open(new File(input));
                SAMFileHeader header = reader.getFileHeader();
                PrintWriter pwriter = null;
                pwriter = new PrintWriter(new FileWriter(output + BAM_HEADER_SUFFIX));
                pwriter.write(header.getTextHeader());
                pwriter.close();
            } catch (IOException e) {
                throw e;
            }
*/

            boolean adjustQuality = alignmentCommandOptions.convertAlignmentCommandOptions.adjustQuality;
            AlignmentAvroSerializer avroSerializer = new AlignmentAvroSerializer(compressionCodecName);
            avroSerializer.toAvro(input, output);

        }
    }

    private void stats() throws IOException {
        // get input parameters
        String input = alignmentCommandOptions.statsAlignmentCommandOptions.input;
        String output = alignmentCommandOptions.statsAlignmentCommandOptions.output;

        try {
            // compute stats using the BamManager
            BamManager alignmentManager = new BamManager(Paths.get(input));
            AlignmentGlobalStats stats = alignmentManager.stats();

            // write results
            PrintWriter writer = new PrintWriter(new File(output + "/stats.json"));
            writer.write(stats.toJSON());
            writer.close();

        } catch (Exception e) {
            throw e;
        }
    }

    private void coverage() throws IOException {
        final int chunkSize = 10000;

        // get input parameters
        String input = alignmentCommandOptions.coverageAlignmentCommandOptions.input;
        String output = alignmentCommandOptions.coverageAlignmentCommandOptions.output;

        Path filePath = Paths.get(input);

        // writer
        PrintWriter writer = new PrintWriter(new File(output + "/" + filePath.getFileName() + ".coverage"));

        SAMFileHeader fileHeader = BamUtils.getFileHeader(filePath);

        AlignmentOptions options = new AlignmentOptions();
        options.setContained(false);

        short[] values;

        BamManager alignmentManager = new BamManager(filePath);
        Iterator<SAMSequenceRecord> iterator = fileHeader.getSequenceDictionary().getSequences().iterator();
        while (iterator.hasNext()) {
            SAMSequenceRecord next = iterator.next();
            for (int i = 0; i < next.getSequenceLength(); i += chunkSize) {
                Region region = new Region(next.getSequenceName(), i + 1,
                        Math.min(i + chunkSize, next.getSequenceLength()));
                RegionCoverage regionCoverage = alignmentManager.coverage(region, options, null);

                // write coverages to file (only values greater than 0)
                values = regionCoverage.getValues();
                for (int j=0, start = region.getStart(); j < values.length; j++, start++) {
                    if (values[j] > 0) {
                        writer.write(next.getSequenceName() + "\t" + start + "\t" + values[j] + "\n");
                    }
                }
            }
        }

        // close
        writer.close();

//
//        HashMap<String, Integer> regionLength = new HashMap<>();
//
//        //TODO: fix using the new RegionCoverage, JT
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
        List<Region> regions = null;
        if (StringUtils.isNotEmpty(alignmentCommandOptions.queryAlignmentCommandOptions.regions)) {
            regions = Region.parseRegions(alignmentCommandOptions.queryAlignmentCommandOptions.regions);
            ad.regionFilter(regions);
        }

        // query for region file
        if (StringUtils.isNotEmpty(alignmentCommandOptions.queryAlignmentCommandOptions.regionFile)) {
            logger.warn("Query for region file, not yet implemented.");
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
        ad.write().format("com.databricks.spark.avro").save(alignmentCommandOptions.queryAlignmentCommandOptions.output);
    }
}
