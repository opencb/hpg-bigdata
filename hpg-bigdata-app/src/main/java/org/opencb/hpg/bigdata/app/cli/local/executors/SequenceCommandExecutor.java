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

package org.opencb.hpg.bigdata.app.cli.local.executors;

import htsjdk.samtools.fastq.FastqReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.opencb.biodata.models.sequence.Read;
import org.opencb.biodata.tools.alignment.stats.SequenceStats;
import org.opencb.biodata.tools.alignment.stats.SequenceStatsCalculator;
import org.opencb.biodata.tools.sequence.FastqRecordToReadBiConverter;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.app.cli.local.options.SequenceCommandOptions;
import org.opencb.hpg.bigdata.core.io.avro.AvroWriter;
import org.opencb.hpg.bigdata.core.utils.CompressionUtils;

import java.io.*;

/**
 * Created by imedina on 03/02/15.
 */
public class SequenceCommandExecutor extends CommandExecutor {

    private SequenceCommandOptions sequenceCommandOptions;

    public SequenceCommandExecutor(SequenceCommandOptions sequenceCommandOptions) {
        super(sequenceCommandOptions.commonCommandOptions);
        this.sequenceCommandOptions = sequenceCommandOptions;
    }

    /**
     * Parse specific 'sequence' command options.
     *
     * @throws IOException Exception thrown if file does not exist
     */
    public void execute() throws IOException {
        String subCommandString = getParsedSubCommand(sequenceCommandOptions.jCommander);
        switch (subCommandString) {
            case "convert":
                convert();
                break;
            case "stats":
                stats();
                break;
            default:
                logger.error("Sequence subcommand '" + subCommandString + "' not valid");
                break;
        }
    }

    private void convert() throws IOException {
        SequenceCommandOptions.ConvertSequenceCommandOptions
                convertSequenceCommandOptions = sequenceCommandOptions.convertSequenceCommandOptions;

        // get input parameters
        String input = convertSequenceCommandOptions.input;
        String output = convertSequenceCommandOptions.output;
        String codecName = convertSequenceCommandOptions.compression;

        try {
            // reader
            FastqReader reader = new FastqReader(new File(input));

            // writer
            OutputStream os = new FileOutputStream(output);

            AvroWriter<Read> writer = new AvroWriter<>(Read.getClassSchema(), CompressionUtils.getAvroCodec(codecName), os);

            // main loop
            FastqRecordToReadBiConverter converter = new FastqRecordToReadBiConverter();
            while (reader.hasNext()) {
                writer.write(converter.to(reader.next()));
            }

            // close
            reader.close();
            writer.close();
            os.close();
        } catch (Exception e) {
            throw e;
        }
    }

    private void stats() throws IOException {
        SequenceCommandOptions.StatsSequenceCommandOptions
                statsSequenceCommandOptions = sequenceCommandOptions.statsSequenceCommandOptions;

        // get input parameters
        String input = statsSequenceCommandOptions.input;
        String output = statsSequenceCommandOptions.output;
        int kvalue = statsSequenceCommandOptions.kmers;

        try {
            // reader
            InputStream is = new FileInputStream(input);
            DataFileStream<Read> reader = new DataFileStream<>(is, new SpecificDatumReader<>(Read.class));

            SequenceStats stats;
            SequenceStats totalStats = new SequenceStats(kvalue);
            SequenceStatsCalculator calculator = new SequenceStatsCalculator();

            // main loop
            for (Read read : reader) {
                stats = calculator.compute(read, kvalue);
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
}
