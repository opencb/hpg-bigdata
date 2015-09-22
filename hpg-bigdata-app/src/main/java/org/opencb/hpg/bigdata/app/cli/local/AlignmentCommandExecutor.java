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
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.tools.alignment.tasks.AlignmentStats;
import org.opencb.biodata.tools.alignment.tasks.AlignmentStatsCalculator;
import org.opencb.biodata.tools.alignment.tasks.RegionDepth;
import org.opencb.biodata.tools.alignment.tasks.RegionDepthCalculator;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.core.NativeSupport;
import org.opencb.hpg.bigdata.core.converters.SAMRecord2ReadAlignmentConverter;

import java.io.*;
import java.util.HashMap;

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
    public void execute() throws IOException {
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
            case "depth":
                init(alignmentCommandOptions.depthAlignmentCommandOptions.commonOptions.logLevel,
                        alignmentCommandOptions.depthAlignmentCommandOptions.commonOptions.verbose,
                        alignmentCommandOptions.depthAlignmentCommandOptions.commonOptions.conf);
                depth();
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
            compressionCodecName = null;
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
            System.out.println("Loading library hpgbigdata...");
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
        }
    }

    private void stats() throws IOException {
        // get input parameters
        String input = alignmentCommandOptions.statsAlignmentCommandOptions.input;
        String output = alignmentCommandOptions.statsAlignmentCommandOptions.output;

        try {
            // reader
            InputStream is = new FileInputStream(input);
            DataFileStream<ReadAlignment> reader = new DataFileStream<>(is, new SpecificDatumReader<>(ReadAlignment.class));

            AlignmentStats stats;
            AlignmentStats totalStats = new AlignmentStats();
            AlignmentStatsCalculator calculator = new AlignmentStatsCalculator();

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

        try {
            // header management
            BufferedReader br = new BufferedReader(new FileReader(input + BAM_HEADER_SUFFIX));
            String line, fieldName, fieldLength;
            String[] fields;
            String[] subfields;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("@SQ")) {
                    fields = line.split("\t");
                    subfields = fields[1].split(":");
                    fieldName = subfields[1];
                    subfields = fields[2].split(":");
                    fieldLength = subfields[1];

                    regionLength.put(fieldName, Integer.parseInt(fieldLength));
                }
            }
            br.close();

            // reader
            InputStream is = new FileInputStream(input);
            DataFileStream<ReadAlignment> reader = new DataFileStream<>(is, new SpecificDatumReader<>(ReadAlignment.class));

            // writer
            PrintWriter writer = new PrintWriter(new File(output + "/depth.txt"));

            String chromName = "";
            int[] chromDepth;

            RegionDepth regionDepth;
            RegionDepthCalculator calculator = new RegionDepthCalculator();

            int pos;
            long prevPos = 0L;

            // main loop
            chromDepth = null;
            for (ReadAlignment readAlignment : reader) {
                if (readAlignment.getAlignment() != null) {
                    regionDepth = calculator.compute(readAlignment);
                    if (chromDepth == null) {
                        chromName = regionDepth.chrom;
                        chromDepth = new int[regionLength.get(regionDepth.chrom)];
                    }
                    if (!chromName.equals(regionDepth.chrom)) {
                        // write depth
                        int length = chromDepth.length;
                        for (int i = 0; i < length; i++) {
                            writer.write(chromName + "\t" + (i + 1) + "\t" + chromDepth[i] + "\n");
                        }

                        // init
                        prevPos = 0L;
                        chromName = regionDepth.chrom;
                        chromDepth = new int[regionLength.get(regionDepth.chrom)];
                    }
                    if (prevPos > regionDepth.position) {
                        throw new IOException("Error: the input file (" + input + ") is not sorted (reads out of order).");
                    }

                    pos = (int) regionDepth.position;
                    for (int i: regionDepth.array) {
                        chromDepth[pos] += regionDepth.array[i];
                        pos++;
                    }
                    prevPos = regionDepth.position;
                }
            }
            // write depth
            int length = chromDepth.length;
            for (int i = 0; i < length; i++) {
                if (chromDepth[i] > 0) {
                    writer.write(chromName + "\t" + (i + 1) + "\t" + chromDepth[i] + "\n");
                }
            }

            // close
            reader.close();
            is.close();
            writer.close();
        } catch (Exception e) {
            throw e;
        }
    }
}
