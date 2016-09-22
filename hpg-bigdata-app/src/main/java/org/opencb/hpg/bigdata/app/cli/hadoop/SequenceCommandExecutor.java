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

package org.opencb.hpg.bigdata.app.cli.hadoop;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.tools.sequence.Fastq2AvroMR;
import org.opencb.hpg.bigdata.tools.sequence.stats.ReadKmersMR;
import org.opencb.hpg.bigdata.tools.sequence.stats.ReadStatsMR;
import org.opencb.hpg.bigdata.core.utils.PathUtils;

/**
 * Created by imedina on 03/02/15.
 */
public class SequenceCommandExecutor extends CommandExecutor {

    private CliOptionsParser.SequenceCommandOptions sequenceCommandOptions;

    public SequenceCommandExecutor(CliOptionsParser.SequenceCommandOptions sequenceCommandOptions) {
        this.sequenceCommandOptions = sequenceCommandOptions;
    }

    /*
     * Parse specific 'sequence' command options
     */
    public void execute() throws Exception {
        String subCommand = sequenceCommandOptions.getParsedSubCommand();

        switch (subCommand) {
            case "convert":
                convert();
                break;
            case "stats":
                stats();
                break;
            case "align":
                System.out.println("Sub-command 'align': Not yet implemented for the command 'sequence' !");
                break;
            default:
                break;
        }
    }

    private void convert() throws Exception {
        CliOptionsParser.ConvertSequenceCommandOptions
                convertSequenceCommandOptions = sequenceCommandOptions.convertSequenceCommandOptions;

        // get input parameters
        String input = convertSequenceCommandOptions.input;
        String output = convertSequenceCommandOptions.output;
        String codecName = convertSequenceCommandOptions.compression;

        // sanity check
        if (codecName.equals("null")) {
            codecName = null;
        }

        // run MapReduce job to convert to GA4GH/Avro model
        try {
            Fastq2AvroMR.run(input, output, codecName);
        } catch (Exception e) {
            throw e;
        }
    }

    private void stats() throws Exception {
        CliOptionsParser.StatsSequenceCommandOptions statsSequenceCommandOptions = sequenceCommandOptions.statsSequenceCommandOptions;

        // get input parameters
        String input = statsSequenceCommandOptions.input;
        String output = statsSequenceCommandOptions.output;
        int kvalue = statsSequenceCommandOptions.kmers;

        // prepare the HDFS output folder
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        String outHdfsDirname = Long.toString(new Date().getTime());

        // run MapReduce job to compute stats
        ReadStatsMR.run(input, outHdfsDirname, kvalue);

        // post-processing
        Path outFile = new Path(outHdfsDirname + "/part-r-00000");

        try {
            if (!fs.exists(outFile)) {
                logger.error("Stats results file not found: {}", outFile.getName());
            } else {
                String outRawFileName =  output + "/stats.json";
                fs.copyToLocalFile(outFile, new Path(outRawFileName));

                //CliUtils.parseStatsFile(outRawFileName, out);
            }
            fs.delete(new Path(outHdfsDirname), true);
        } catch (IOException e) {
            throw e;
        }
    }

    @Deprecated
    private void kmers(String input, String output, int kvalue) throws Exception {
        // clean paths
        String in = PathUtils.clean(input);
        String out = PathUtils.clean(output);

        if (!PathUtils.isHdfs(input)) {
            throw new IOException("To run fastq kmers, input files '" + input
                    + "' must be stored in the HDFS/Haddop. Use the command 'convert fastq2sa' to import your file.");
        }

        try {
            ReadKmersMR.run(in, out, kvalue);
        } catch (Exception e) {
            throw e;
        }
    }
}
