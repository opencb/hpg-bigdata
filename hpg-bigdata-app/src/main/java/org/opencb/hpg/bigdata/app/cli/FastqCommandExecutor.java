package org.opencb.hpg.bigdata.app.cli;

import org.opencb.hpg.bigdata.core.io.FastqKmersMR;
import org.opencb.hpg.bigdata.core.io.FastqStatsMR;
import org.opencb.hpg.bigdata.core.utils.PathUtils;

/**
 * Created by imedina on 03/02/15.
 */
public class FastqCommandExecutor extends CommandExecutor {

    private CliOptionsParser.FastqCommandOptions fastqCommandOptions;

    public FastqCommandExecutor(CliOptionsParser.FastqCommandOptions fastqCommandOptions) {
        super(fastqCommandOptions.commonOptions.logLevel, fastqCommandOptions.commonOptions.verbose,
                fastqCommandOptions.commonOptions.conf);

        this.fastqCommandOptions = fastqCommandOptions;
    }


    /**
     * Parse specific 'fastq' command options
     */
    public void execute() {
        logger.info("Executing {} CLI options", "fastq");
        
        if (fastqCommandOptions.stats) {
        	stats(fastqCommandOptions.input, fastqCommandOptions.output, fastqCommandOptions.kmers);
        } else if (fastqCommandOptions.kmers > 0) {
        	kmers(fastqCommandOptions.input, fastqCommandOptions.output, fastqCommandOptions.kmers);
        }
    }


	private void stats(String input, String output, int kvalue) {
		// clean paths
		String in = PathUtils.clean(input);
		String out = PathUtils.clean(output);

		if (!PathUtils.isHdfs(input)) {
			logger.error("To run fastq stats, input files '{}' must be stored in the HDFS/Haddop. Use the command 'ga4gh fastq2sa' to import your file.", input);
			System.exit(-1);
		}
		
		try {
			FastqStatsMR.run(in, out, kvalue);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void kmers(String input, String output, int kvalue) {
		// clean paths
		String in = PathUtils.clean(input);
		String out = PathUtils.clean(output);

		if (!PathUtils.isHdfs(input)) {
			logger.error("To run fastq kmers, input files '{}' must be stored in the HDFS/Haddop. Use the command 'ga4gh fastq2sa' to import your file.", input);
			System.exit(-1);
		}
		
		try {
			FastqKmersMR.run(in, out, kvalue);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	
}
