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

package org.opencb.hpg.bigdata.app.cli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.opencb.hpg.bigdata.core.utils.PathUtils;
import org.opencb.hpg.bigdata.core.AlignerParams;
import org.opencb.hpg.bigdata.tools.tasks.read.mr.ReadAlignMR;

import java.io.IOException;
import java.util.Date;

/**
 * Created by imedina on 03/02/15.
 */
public class FastqAlignCommandExecutor extends CommandExecutor {

	private CliOptionsParser.FastqAlignCommandOptions fastqAlignCommandOptions;

	public FastqAlignCommandExecutor(CliOptionsParser.FastqAlignCommandOptions fastqAlignCommandOptions) {
		super(fastqAlignCommandOptions.commonOptions.logLevel, fastqAlignCommandOptions.commonOptions.verbose,
				fastqAlignCommandOptions.commonOptions.conf);

		this.fastqAlignCommandOptions = fastqAlignCommandOptions;
	}

	/**
	 * Parse specific 'fastq' command options
	 */
	public void execute() {
		logger.info("Executing {} CLI options", "fastq");

		// prepare the HDFS output folder
		FileSystem fs = null;
		Configuration conf = new Configuration();
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		String outHdfsDirname = new String("" + new Date().getTime());

		// clean paths
		String in = PathUtils.clean(fastqAlignCommandOptions.input);
		String index = PathUtils.clean(fastqAlignCommandOptions.index);
		String out = PathUtils.clean(fastqAlignCommandOptions.output);

		if (!PathUtils.isHdfs(fastqAlignCommandOptions.input)) {
			logger.error("To align fastq, the input FastQ file '{}' must be stored in the HDFS/Haddop. Use the command 'convert fastq2sa' to import your file.", fastqAlignCommandOptions.input);
			System.exit(-1);
		}
/*
		if (!PathUtils.isHdfs(fastqAlignCommandOptions.index)) {
			logger.error("To align fastq, the index folder '{}' must be stored in the HDFS/Haddop.", fastqAlignCommandOptions.index);
			System.exit(-1);
		}
*/
		if (!PathUtils.isHdfs(fastqAlignCommandOptions.output)) {
			logger.error("To align fastq, the output folder '{}' must be stored in the HDFS/Haddop.", fastqAlignCommandOptions.output);
			System.exit(-1);
		}

		try {
			System.out.println("input = " + in + ", index = " + index + ", out = " + out);

			AlignerParams params = new AlignerParams();
			params.seqFileName1 = in;
			params.indexFolderName = index;
			params.resultFileName = out;
			ReadAlignMR.run(params);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
