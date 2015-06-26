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
import org.ga4gh.models.ReadAlignment;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.tools.stats.alignment.mr.ReadAlignmentDepthMR;
import org.opencb.hpg.bigdata.tools.stats.alignment.mr.ReadAlignmentSortMR;
import org.opencb.hpg.bigdata.tools.stats.alignment.mr.ReadAlignmentStatsMR;
import org.opencb.hpg.bigdata.tools.io.parquet.ParquetMR;
import org.opencb.hpg.bigdata.core.utils.PathUtils;

/**
 * Created by imedina on 15/03/15.
 */
@Deprecated
public class BamCommandExecutor extends CommandExecutor {

	private CliOptionsParser.AlignmentCommandOptions alignmentCommandOptions;

	public BamCommandExecutor(CliOptionsParser.AlignmentCommandOptions alignmentCommandOptions) {
//        super(alignmentCommandOptions.logLevel);

		this.alignmentCommandOptions = alignmentCommandOptions;
	}


	/**
	 * Parse specific 'bam' command options
	 */
	public void execute() {
		logger.info("Executing {} CLI options", "bam");

		// prepare the HDFS output folder
		FileSystem fs = null;
		Configuration conf = new Configuration();
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		String outHdfsDirname = new String("" + new Date().getTime());

		String outdir = null;
		String outName = null;
		switch(alignmentCommandOptions.getParsedSubCommand()) {
			case "stats": {
                outdir = alignmentCommandOptions.statsAlignmentCommandOptions.output;
				outName = "stats.json";
				stats(alignmentCommandOptions.statsAlignmentCommandOptions.input, outHdfsDirname);
				break;
			}
			case "depth": {
				outName = "depth.txt";
//				depth(alignmentCommandOptions.input, outHdfsDirname);
				break;
			}
			case "sort": {
//				sort(alignmentCommandOptions.input, alignmentCommandOptions.output);
				return;
			}
			case "to-parquet": {
                outdir = alignmentCommandOptions.convertAlignmentCommandOptions.output;
				toParquet(alignmentCommandOptions.convertAlignmentCommandOptions.input, alignmentCommandOptions.convertAlignmentCommandOptions.output, alignmentCommandOptions.convertAlignmentCommandOptions.compression);
				return;
			}
			default: {
				logger.error("Error: BAM/SAM command not yet implemented");
				System.exit(-1);
			}
		}

		// post-processing
		Path outFile = new Path(outHdfsDirname + "/part-r-00000");

		try {
			if (!fs.exists(outFile)) {
				System.out.println("out file = " + outFile.getName() + " does not exist !!");
			} else {
				String outRawFileName = outdir + "/" + outName;
				fs.copyToLocalFile(outFile, new Path(outRawFileName));

//				if (alignmentCommandOptions.command.equalsIgnoreCase("depth")) {
//					Path outJson = new Path(outHdfsDirname + ".summary.depth.json");
//					fs.copyToLocalFile(outJson, new Path(alignmentCommandOptions.output + "/depth.summary.json"));
//					fs.delete(outJson, true);
//				}

				//Utils.parseStatsFile(outRawFileName, out);
			}
			fs.delete(new Path(outHdfsDirname), true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	private void stats(String input, String output) {
		// clean paths
		String in = PathUtils.clean(input);
		String out = PathUtils.clean(output);

		if (!PathUtils.isHdfs(input)) {
			logger.error("To run BAM stats, input files '{}' must be stored in the HDFS/Haddop. Use the command 'convert bam2sa' to import your file.", input);
			System.exit(-1);
		}

		try {
			ReadAlignmentStatsMR.run(in, out);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void depth(String input, String output) {
		// clean paths
		String in = PathUtils.clean(input);
		String out = PathUtils.clean(output);

		if (!PathUtils.isHdfs(input)) {
			logger.error("To run BAM stats, input files '{}' must be stored in the HDFS/Hadoop. Use the command 'convert bam2sa' to import your file.", input);
			System.exit(-1);
		}

		try {
			ReadAlignmentDepthMR.run(in, out);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void sort(String input, String output) {
		// clean paths
		String in = PathUtils.clean(input);
		String out = PathUtils.clean(output);

		if (!PathUtils.isHdfs(input)) {
			logger.error("To run BAM stats, input files '{}' must be stored in the HDFS/Haddop. Use the command 'convert bam2sa' to import your file.", input);
			System.exit(-1);
		}

		if (!PathUtils.isHdfs(output)) {
			logger.error("To run BAM stats, output directory '{}' must be a HDFS/Hadoop.", output);
			System.exit(-1);
		}

		try {
			ReadAlignmentSortMR.run(in, out);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void toParquet(String input, String output, String codecName) {
		// clean paths
		String in = PathUtils.clean(input);
		String out = PathUtils.clean(output);

		if (!PathUtils.isHdfs(input)) {
			logger.error("To run BAM stats, input files '{}' must be stored in the HDFS/Haddop. Use the command 'convert bam2sa' to import your file.", input);
			System.exit(-1);
		}

		if (!PathUtils.isHdfs(output)) {
			logger.error("To run BAM stats, output directory '{}' must be a HDFS/Hadoop.", output);
			System.exit(-1);
		}

		try {
			new ParquetMR(ReadAlignment.getClassSchema()).run(in, out, codecName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
