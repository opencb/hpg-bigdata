package org.opencb.hpg.bigdata.app.cli;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.opencb.hpg.bigdata.core.io.ReadAlignmentDepthMR;
import org.opencb.hpg.bigdata.core.io.ReadAlignmentSortMR;
import org.opencb.hpg.bigdata.core.io.ReadAlignmentStatsMR;
import org.opencb.hpg.bigdata.core.utils.PathUtils;

/**
 * Created by imedina on 15/03/15.
 */
public class BamCommandExecutor extends CommandExecutor {

    private CliOptionsParser.BamCommandOptions bamCommandOptions;

    public BamCommandExecutor(CliOptionsParser.BamCommandOptions bamCommandOptions) {
        super(bamCommandOptions.commonOptions.logLevel, bamCommandOptions.commonOptions.verbose,
                bamCommandOptions.commonOptions.conf);

        this.bamCommandOptions = bamCommandOptions;
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

		String outName = null;
		switch(bamCommandOptions.command) {
		case "stats": {
			outName = "stats.json";
			stats(bamCommandOptions.input, outHdfsDirname);
			break;
		}
		case "depth": {
			outName = "depth.txt";
			depth(bamCommandOptions.input, outHdfsDirname);
			break;
		}
		case "sort": {
			sort(bamCommandOptions.input, bamCommandOptions.output);
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
				String outRawFileName =  bamCommandOptions.output + "/" + outName;
				fs.copyToLocalFile(outFile, new Path(outRawFileName));
				
				if (bamCommandOptions.command.equalsIgnoreCase("depth")) {
					Path outJson = new Path(outHdfsDirname + ".summary.depth.json");
					fs.copyToLocalFile(outJson, new Path(bamCommandOptions.output + "/depth.summary.json"));
					fs.delete(outJson, true);
				}

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
			logger.error("To run BAM stats, input files '{}' must be stored in the HDFS/Haddop. Use the command 'ga4gh bam2sa' to import your file.", input);
			System.exit(-1);
		}

		try {
			ReadAlignmentStatsMR.run(in, out);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void sort(String input, String output) {
		// clean paths
		String in = PathUtils.clean(input);
		String out = PathUtils.clean(output);

		if (!PathUtils.isHdfs(input)) {
			logger.error("To run BAM stats, input files '{}' must be stored in the HDFS/Haddop. Use the command 'ga4gh bam2sa' to import your file.", input);
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
	
	private void depth(String input, String output) {
		// clean paths
		String in = PathUtils.clean(input);
		String out = PathUtils.clean(output);

		if (!PathUtils.isHdfs(input)) {
			logger.error("To run BAM stats, input files '{}' must be stored in the HDFS/Hadoop. Use the command 'ga4gh bam2sa' to import your file.", input);
			System.exit(-1);
		}

		try {
			ReadAlignmentDepthMR.run(in, out);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
