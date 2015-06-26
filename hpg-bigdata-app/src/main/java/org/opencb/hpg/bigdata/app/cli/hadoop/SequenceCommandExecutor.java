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

import com.beust.jcommander.JCommander;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.tools.stats.read.mr.ReadKmersMR;
import org.opencb.hpg.bigdata.tools.stats.read.mr.ReadStatsMR;
import org.opencb.hpg.bigdata.core.utils.PathUtils;

/**
 * Created by imedina on 03/02/15.
 */
public class SequenceCommandExecutor extends CommandExecutor {

	private CliOptionsParser.SequenceCommandOptions sequenceCommandOptions;

	public SequenceCommandExecutor(CliOptionsParser.SequenceCommandOptions sequenceCommandOptions) {
//		super(fastqCommandOptions.logLevel, fastqCommandOptions.verbose, fastqCommandOptions.conf);

		this.sequenceCommandOptions = sequenceCommandOptions;
	}


	/**
	 * Parse specific 'fastq' command options
	 */
	public void execute() {
//		logger.info("Executing {} CLI options", "fastq");

		// prepare the HDFS output folder
		FileSystem fs = null;
		Configuration conf = new Configuration();
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		String outHdfsDirname = new String("" + new Date().getTime());

		String subCommand = sequenceCommandOptions.getParsedSubCommand();



        switch (subCommand) {
            case "convert":
                System.out.println("convert");
				convert();
                break;
			case "stats":
				System.out.println("stats");
				break;
			case "align":
				System.out.println("align");
				break;
			default:
				break;
        }

//		if (fastqCommandOptions.stats) {
//			stats(fastqCommandOptions.input, outHdfsDirname, fastqCommandOptions.kmers);
//		} else if (fastqCommandOptions.kmers > 0) {
//			kmers(fastqCommandOptions.input, outHdfsDirname, fastqCommandOptions.kmers);
//		} else {
//			logger.error("Error: FastQ command not yet implemented");
//			System.exit(-1);
//		}
//
//		// post-processing
//		Path outFile = new Path(outHdfsDirname + "/part-r-00000");
//
//		try {
//			if (!fs.exists(outFile)) {
//				System.out.println("out file = " + outFile.getName() + " does not exist !!");
//			} else {
//				String outRawFileName =  fastqCommandOptions.output + "/raw.json";
//				fs.copyToLocalFile(outFile, new Path(outRawFileName));
//
//				//Utils.parseStatsFile(outRawFileName, out);
//			}
//			fs.delete(new Path(outHdfsDirname), true);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	}


	private void convert() {
		CliOptionsParser.ConvertSequenceCommandOptions convertSequenceCommandOptions = sequenceCommandOptions.convertSequenceCommandOptions;

	}

	private void stats(String input, String output, int kvalue) {
		// clean paths
		String in = PathUtils.clean(input);
		String out = PathUtils.clean(output);

		if (!PathUtils.isHdfs(input)) {
			logger.error("To run fastq stats, input files '{}' must be stored in the HDFS/Haddop. Use the command 'convert fastq2sa' to import your file.", input);
			System.exit(-1);
		}

		try {
			ReadStatsMR.run(in, out, kvalue);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void kmers(String input, String output, int kvalue) {
		// clean paths
		String in = PathUtils.clean(input);
		String out = PathUtils.clean(output);

		if (!PathUtils.isHdfs(input)) {
			logger.error("To run fastq kmers, input files '{}' must be stored in the HDFS/Haddop. Use the command 'convert fastq2sa' to import your file.", input);
			System.exit(-1);
		}

		try {
			ReadKmersMR.run(in, out, kvalue);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	
}
