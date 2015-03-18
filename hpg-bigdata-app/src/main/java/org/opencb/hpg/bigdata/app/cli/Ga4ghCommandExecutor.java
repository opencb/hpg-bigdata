package org.opencb.hpg.bigdata.app.cli;

import htsjdk.samtools.fastq.FastqReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.opencb.ga4gh.models.Read;
import org.opencb.ga4gh.utils.ReadUtils;
import org.opencb.hpg.bigdata.core.converters.FastqRecord2ReadConverter;
import org.opencb.hpg.bigdata.core.io.AvroWriter;

/**
 * Created by imedina on 16/03/15.
 */
public class Ga4ghCommandExecutor extends CommandExecutor {

	private final static String FASTQ_2_GA = "fastq2ga"; 
	private final static String GA_2_FASTQ = "ga2fastq"; 
	private final static String SAM_2_GA   = "sam2ga"; 
	private final static String GA_2_SAM   = "ga2sam"; 
	private final static String BAM_2_GA   = "bam2ga"; 
	private final static String GA_2_BAM   = "ga2bam"; 

	private final static String FASTQ_2_GA_DESC = "Save Fastq file as Global Alliance for Genomics and Health (ga4gh) in Avro format"; 
	private final static String GA_2_FASTQ_DESC = "Save Global Alliance for Genomics and Health (ga4gh) in Avro format as Fastq file"; 
	private final static String SAM_2_GA_DESC   = "Save SAM file as Global Alliance for Genomics and Health (ga4gh) in Avro format"; 
	private final static String GA_2_SAM_DESC   = "Save Global Alliance for Genomics and Health (ga4gh) in Avro format as SAM file";
	private final static String BAM_2_GA_DESC   = "Save BAM file as Global Alliance for Genomics and Health (ga4gh) in Avro format"; 
	private final static String GA_2_BAM_DESC   = "Save Global Alliance for Genomics and Health (ga4gh) in Avro format as BAM file";

	private CliOptionsParser.Ga4ghCommandOptions ga4ghCommandOptions;

	public Ga4ghCommandExecutor(CliOptionsParser.Ga4ghCommandOptions ga4ghCommandOptions) {
		super(ga4ghCommandOptions.commonOptions.logLevel, ga4ghCommandOptions.commonOptions.verbose,
				ga4ghCommandOptions.commonOptions.conf);

		this.ga4ghCommandOptions = ga4ghCommandOptions;
	}


	/**
	 * Parse specific 'ga4gh' command options
	 */
	public void execute() {
		logger.info("Executing {} CLI options", "ga4gh");

		switch (ga4ghCommandOptions.conversion) {
		case FASTQ_2_GA: {
			try {
				fastq2ga(ga4ghCommandOptions.input, ga4ghCommandOptions.output);
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		}
		case GA_2_FASTQ: {
			try {
				ga2fastq(ga4ghCommandOptions.input, ga4ghCommandOptions.output);
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		}
		default: {
			logger.error("Invalid conversion {}", ga4ghCommandOptions.conversion);
			System.out.println("Invalid conversion (" + ga4ghCommandOptions.conversion + "). Valid conversions are:\n" + getValidConversionString());
			break;
		}
		}

		logger.debug("Input file: {}", ga4ghCommandOptions.input);
	}

	private void fastq2ga(String input, String output) throws IOException {
		// for hadoop
		// Configuration config = new Configuration();
		// FileSystem hdfs = FileSystem.get(config);
		// OutputStream is = hdfs.create(new Path(output));

		// reader
		FastqRecord2ReadConverter converter = new FastqRecord2ReadConverter();
		FastqReader fqReader = new FastqReader(new File(input));

		// writer
		OutputStream os = new FileOutputStream(output);
		AvroWriter<Read> writer = new AvroWriter<>(Read.getClassSchema(), CodecFactory.snappyCodec(), os);

		// main loop
		while (fqReader.hasNext()) {
			writer.write(converter.forward(fqReader.next()));
		}

		// close
		fqReader.close();
		writer.close();
		os.close();
	}

	private void ga2fastq(String input, String output) throws IOException {	
		// for hadoop
		// Configuration config = new Configuration();
		// FileSystem hdfs = FileSystem.get(config);
		// InputStream is = hdfs.open(new Path(input));
		
		// reader
		InputStream is = new FileInputStream(input);
		DataFileStream<Read> reader = new DataFileStream<Read>(is, new SpecificDatumReader<Read>(Read.class));

		// writer
		PrintWriter writer = new PrintWriter(new FileWriter(output));
		
		for (Read read: reader) {
			writer.write(ReadUtils.getFastqString(read));
		}
		
		// close
		reader.close();
		writer.close();
		is.close();
	}

	private String getValidConversionString() {
		String res = new String();
		res += "\t- " + FASTQ_2_GA + "\t" + FASTQ_2_GA_DESC + "\n";
		res += "\t- " + GA_2_FASTQ + "\t" + GA_2_FASTQ_DESC + "\n";
		res += "\t- " + SAM_2_GA + "\t" + SAM_2_GA_DESC + "\n";
		res += "\t- " + GA_2_SAM + "\t" + GA_2_SAM_DESC + "\n";
		res += "\t- " + BAM_2_GA + "\t" + BAM_2_GA_DESC + "\n";
		res += "\t- " + GA_2_BAM + "\t" + GA_2_BAM_DESC + "\n";
		return res;

	}
}
