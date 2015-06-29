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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.tools.converters.mr.Bam2AvroMR;
import org.opencb.hpg.bigdata.tools.stats.alignment.mr.ReadAlignmentDepthMR;
import org.opencb.hpg.bigdata.tools.stats.alignment.mr.ReadAlignmentStatsMR;

import java.io.IOException;
import java.util.Date;

/**
 * Created by imedina on 16/03/15.
 */
public class AlignmentCommandExecutor extends CommandExecutor {

    private CliOptionsParser.AlignmentCommandOptions alignmentCommandOptions;

    public AlignmentCommandExecutor(CliOptionsParser.AlignmentCommandOptions alignmentCommandOptions) {
        this.alignmentCommandOptions = alignmentCommandOptions;
    }

    /**
     * Parse specific 'alignment' command options
     */
    public void execute() {
        String subCommand = alignmentCommandOptions.getParsedSubCommand();

        switch (subCommand) {
            case "convert":
                convert();
                break;
            case "stats":
                stats();
                break;
            case "depth":
                depth();
                break;
            case "align":
                System.out.println("Sub-command 'align': Not yet implemented for the command 'alignment' !");
                break;
            default:
                break;
        }
    }

    private void convert() {
        CliOptionsParser.ConvertAlignmentCommandOptions convertAlignmentCommandOptions = alignmentCommandOptions.convertAlignmentCommandOptions;

        // get input parameters
        String input = convertAlignmentCommandOptions.input;
        String output = convertAlignmentCommandOptions.output;
        String codecName = convertAlignmentCommandOptions.compression;

        // sanity check
        if (codecName.equals("null")) {
            codecName = null;
        }

        // run MapReduce job to convert to GA4GH/Avro model
        try {
            Bam2AvroMR.run(input, output, codecName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void stats() {
        CliOptionsParser.StatsAlignmentCommandOptions statsAlignmentCommandOptions = alignmentCommandOptions.statsAlignmentCommandOptions;

        // get input parameters
        String input = statsAlignmentCommandOptions.input;
        String output = statsAlignmentCommandOptions.output;

        // prepare the HDFS output folder
        FileSystem fs = null;
        Configuration conf = new Configuration();
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String outHdfsDirname = new String("" + new Date().getTime());

        // run MapReduce job to compute stats
        try {
            ReadAlignmentStatsMR.run(input, outHdfsDirname);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // post-processing
        Path outFile = new Path(outHdfsDirname + "/part-r-00000");

        try {
            if (!fs.exists(outFile)) {
                logger.error("Stats results file not found: {}", outFile.getName());
            } else {
                String outRawFileName =  output + "/stats.json";
                fs.copyToLocalFile(outFile, new Path(outRawFileName));

                //Utils.parseStatsFile(outRawFileName, out);
            }
            fs.delete(new Path(outHdfsDirname), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void depth() {
        CliOptionsParser.DepthAlignmentCommandOptions depthAlignmentCommandOptions = alignmentCommandOptions.depthAlignmentCommandOptions;

        // get input parameters
        String input = depthAlignmentCommandOptions.input;
        String output = depthAlignmentCommandOptions.output;

        // prepare the HDFS output folder
        FileSystem fs = null;
        Configuration conf = new Configuration();
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String outHdfsDirname = new String("" + new Date().getTime());

        // run MapReduce job to compute stats
        try {
            ReadAlignmentDepthMR.run(input, outHdfsDirname);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // post-processing
        Path outFile = new Path(outHdfsDirname + "/part-r-00000");

        try {
            if (!fs.exists(outFile)) {
                logger.error("Stats results file not found: {}", outFile.getName());
            } else {
                String outRawFileName =  output + "/depth.txt";
                fs.copyToLocalFile(outFile, new Path(outRawFileName));

                //Utils.parseStatsFile(outRawFileName, out);
            }
            fs.delete(new Path(outHdfsDirname), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
