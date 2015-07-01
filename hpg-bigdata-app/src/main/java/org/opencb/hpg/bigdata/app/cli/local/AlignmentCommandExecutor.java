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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ga4gh.models.LinearAlignment;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.tools.alignment.tasks.AlignmentStats;
import org.opencb.biodata.tools.alignment.tasks.AlignmentStatsCalculator;
import org.opencb.biodata.tools.alignment.tasks.RegionDepth;
import org.opencb.biodata.tools.alignment.tasks.RegionDepthCalculator;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.app.cli.hadoop.CliOptionsParser;
import org.opencb.hpg.bigdata.core.NativeSupport;
import org.opencb.hpg.bigdata.core.converters.SAMRecord2ReadAlignmentConverter;
import org.opencb.hpg.bigdata.core.utils.PathUtils;
import org.opencb.hpg.bigdata.tools.converters.mr.Bam2AvroMR;
import org.opencb.hpg.bigdata.tools.converters.mr.ChunkKey;
import org.opencb.hpg.bigdata.tools.io.RegionDepthWritable;
import org.opencb.hpg.bigdata.tools.io.parquet.ParquetMR;
import org.opencb.hpg.bigdata.tools.stats.alignment.mr.ReadAlignmentDepthMR;
import org.opencb.hpg.bigdata.tools.stats.alignment.mr.ReadAlignmentStatsMR;

import java.io.*;
import java.util.Date;

/**
 * Created by imedina on 16/03/15.
 */
public class AlignmentCommandExecutor extends CommandExecutor {

    private LocalCliOptionsParser.AlignmentCommandOptions alignmentCommandOptions;

    public final static String BAM_HEADER_SUFFIX = ".header";

    public AlignmentCommandExecutor(LocalCliOptionsParser.AlignmentCommandOptions alignmentCommandOptions) {
        this.alignmentCommandOptions = alignmentCommandOptions;
    }

    /**
     * Parse specific 'alignment' command options
     */
    public void execute() {
        String subCommand = alignmentCommandOptions.getParsedSubCommand();

        switch (subCommand) {
            case "convert":
                init(alignmentCommandOptions.convertAlignmentCommandOptions.commonOptions.logLevel,
                        alignmentCommandOptions.convertAlignmentCommandOptions.commonOptions.verbose,
                        alignmentCommandOptions.convertAlignmentCommandOptions.commonOptions.conf);
                convert();
                break;
/*
            case "stats":
                init(alignmentCommandOptions.statsAlignmentCommandOptions.commonOptions.logLevel,
                        alignmentCommandOptions.statsAlignmentCommandOptions.commonOptions.verbose,
                        alignmentCommandOptions.statsAlignmentCommandOptions.commonOptions.conf);
                stats();
                break;
            case "depth":
                depth();
                break;
            case "align":
                System.out.println("Sub-command 'align': Not yet implemented for the command 'alignment' !");
                break;
*/
            default:
                break;
        }
    }

    private void convert() {
        String input = alignmentCommandOptions.convertAlignmentCommandOptions.input;
        String output = alignmentCommandOptions.convertAlignmentCommandOptions.output;
        String compressionCodecName = alignmentCommandOptions.convertAlignmentCommandOptions.compression;

        // sanity check
        if (compressionCodecName.equals("null")) {
            compressionCodecName = null;
        }

        if (alignmentCommandOptions.convertAlignmentCommandOptions.toBam) {
            // conversion: GA4GH/Avro model -> BAM

            try {
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
                DataFileStream<ReadAlignment> reader = new DataFileStream<ReadAlignment>(is, new SpecificDatumReader<ReadAlignment>(ReadAlignment.class));

                // writer
                OutputStream os = new FileOutputStream(new File(output));
                SAMFileWriter writer = new SAMFileWriterFactory().makeBAMWriter(header, false, new File(output));

                // main loop
                SAMRecord samRecord;
                SAMRecord2ReadAlignmentConverter converter = new SAMRecord2ReadAlignmentConverter();
                for (ReadAlignment readAlignment : reader) {
                    samRecord = converter.backward(readAlignment);
                    samRecord.setHeader(header);
                    writer.addAlignment(samRecord);
                }

                // close
                reader.close();
                writer.close();
                os.close();
                is.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

            return;
        }

        // conversion: BAM -> GA4GH/Avro model
        System.out.println("Loading library hpgbigdata...");
        System.out.println("\tjava.libary.path = " + System.getProperty("java.library.path"));
        System.loadLibrary("hpgbigdata");
        System.out.println("...done!");
        new NativeSupport().bam2ga(input, output, compressionCodecName == null ? "snappy" : compressionCodecName);

        try {
            // header management: saved it in a separate file
            SamReader reader = SamReaderFactory.makeDefault().open(new File(input));
            SAMFileHeader header = reader.getFileHeader();
            PrintWriter pwriter = null;
            pwriter = new PrintWriter(new FileWriter(output + BAM_HEADER_SUFFIX));
            pwriter.write(header.getTextHeader());
            pwriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void stats() {
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
			e.printStackTrace();
		}
    }

    private void depth() {
        /*
        // get input parameters
        String input = alignmentCommandOptions.statsAlignmentCommandOptions.input;
        String output = alignmentCommandOptions.statsAlignmentCommandOptions.output;

        try {
            // reader
            InputStream is = new FileInputStream(input);
            DataFileStream<ReadAlignment> reader = new DataFileStream<>(is, new SpecificDatumReader<>(ReadAlignment.class));

            RegionDepth regionDepth;
            RegionDepth summaryRegionDepth;
            RegionDepthCalculator calculator = new RegionDepthCalculator();

            // main loop
            for (ReadAlignment readAlignment : reader) {
                if (readAlignment.getAlignment() != null) {
                    regionDepth = calculator.compute(readAlignment);
                }
            }

            // close reader
            reader.close();
            is.close();

            // write results
            PrintWriter writer = new PrintWriter(new File(output + "/stats.json"));
            writer.write(totalStats.toJSON());
            writer.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        */
    }
}
