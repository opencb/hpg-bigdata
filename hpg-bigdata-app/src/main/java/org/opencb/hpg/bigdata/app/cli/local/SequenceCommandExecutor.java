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

import htsjdk.samtools.fastq.FastqReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.opencb.biodata.models.sequence.Read;
import org.opencb.biodata.tools.alignment.stats.SequenceStats;
import org.opencb.biodata.tools.alignment.stats.SequenceStatsCalculator;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.core.converters.FastqRecord2ReadConverter;
import org.opencb.hpg.bigdata.core.io.avro.AvroWriter;
import org.opencb.hpg.bigdata.core.utils.AvroUtils;

import java.io.*;

/**
 * Created by imedina on 03/02/15.
 */
public class SequenceCommandExecutor extends CommandExecutor {

    private LocalCliOptionsParser.SequenceCommandOptions sequenceCommandOptions;

    public SequenceCommandExecutor(LocalCliOptionsParser.SequenceCommandOptions sequenceCommandOptions) {
        this.sequenceCommandOptions = sequenceCommandOptions;
    }

    /**
     * Parse specific 'sequence' command options.
     *
     * @throws IOException Exception thrown if file does not exist
     */
    public void execute() throws IOException {
        String subCommand = sequenceCommandOptions.getParsedSubCommand();
        switch (subCommand) {
            case "convert":
                convert();
                break;
            case "stats":
                stats();
                break;
            default:
                break;
        }
    }

    private void convert() throws IOException {
        LocalCliOptionsParser.ConvertSequenceCommandOptions
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

            AvroWriter<Read> writer = new AvroWriter<>(Read.getClassSchema(), AvroUtils.getCodec(codecName), os);

            // main loop
            FastqRecord2ReadConverter converter = new FastqRecord2ReadConverter();
            while (reader.hasNext()) {
                writer.write(converter.forward(reader.next()));
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
        LocalCliOptionsParser.StatsSequenceCommandOptions statsSequenceCommandOptions = sequenceCommandOptions.statsSequenceCommandOptions;

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
