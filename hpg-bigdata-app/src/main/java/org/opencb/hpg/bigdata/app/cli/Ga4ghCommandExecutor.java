package org.opencb.hpg.bigdata.app.cli;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.fastq.FastqReader;
import htsjdk.samtools.util.LineReader;
import htsjdk.samtools.util.StringLineReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.ga4gh.models.ReadAlignment;
import org.opencb.ga4gh.models.Read;
import org.opencb.ga4gh.utils.ReadUtils;
import org.opencb.hpg.bigdata.core.converters.FastqRecord2ReadConverter;
import org.opencb.hpg.bigdata.core.converters.SAMRecord2ReadAlignmentConverter;
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

	private final static String SAM_HEADER_SUFFIX = ".header";

	private final static int SAM_FLAG  = 0;
	private final static int BAM_FLAG  = 1;
	private final static int CRAM_FLAG = 2;

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
		case SAM_2_GA: {
			try {
				sam2ga(ga4ghCommandOptions.input, ga4ghCommandOptions.output);
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		}
		case GA_2_SAM: {
			try {
				ga2sam(ga4ghCommandOptions.input, ga4ghCommandOptions.output, SAM_FLAG);
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		}
		case BAM_2_GA: {
			try {
				sam2ga(ga4ghCommandOptions.input, ga4ghCommandOptions.output);
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		}
		case GA_2_BAM: {
			try {
				ga2sam(ga4ghCommandOptions.input, ga4ghCommandOptions.output, BAM_FLAG);
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
		FastqReader reader = new FastqReader(new File(input));

		// writer
		OutputStream os = new FileOutputStream(output);
		AvroWriter<Read> writer = new AvroWriter<>(Read.getClassSchema(), CodecFactory.snappyCodec(), os);

		// main loop
		FastqRecord2ReadConverter converter = new FastqRecord2ReadConverter();
		while (reader.hasNext()) {
			writer.write(converter.forward(reader.next()));
		}

		// close
		reader.close();
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

		// main loop
		for (Read read: reader) {
			writer.write(ReadUtils.getFastqString(read));
		}

		// close
		reader.close();
		writer.close();
		is.close();
	}

	private void sam2ga(String input, String output) throws IOException {
		// reader
		SamReader reader = SamReaderFactory.makeDefault().open(new File(input));

		// header management: saved in a separate file
		SAMFileHeader header = reader.getFileHeader();
		PrintWriter pwriter = new PrintWriter(new FileWriter(output + SAM_HEADER_SUFFIX));
		pwriter.write(header.getTextHeader());
		pwriter.close();

		// writer
		OutputStream os = new FileOutputStream(output);
		AvroWriter<ReadAlignment> writer = new AvroWriter<ReadAlignment>(ReadAlignment.getClassSchema(), CodecFactory.snappyCodec(), os);

		// main loop
		SAMRecord2ReadAlignmentConverter converter = new SAMRecord2ReadAlignmentConverter();
		for (final SAMRecord samRecord : reader) {
			writer.write(converter.forward(samRecord));
		}

		// close
		reader.close();
		writer.close();
		os.close();
	}

	private void ga2sam(String input, String output, int flag) throws IOException {
		// header management: read it from a separate file
		File file = new File(input + SAM_HEADER_SUFFIX);
		FileInputStream fis = new FileInputStream(file);
		byte[] data = new byte[(int) file.length()];
		fis.read(data);
		fis.close();

		String textHeader = new String(data);

		SAMFileHeader header = new SAMFileHeader();
		LineReader lineReader = new StringLineReader(textHeader);
		header = new SAMTextHeaderCodec().decode(lineReader, textHeader);

		// reader
		InputStream is = new FileInputStream(input);
		DataFileStream<ReadAlignment> reader = new DataFileStream<ReadAlignment>(is, new SpecificDatumReader<ReadAlignment>(ReadAlignment.class));

		// writer
		SAMFileWriter writer = null;
		switch (flag) {
		case SAM_FLAG: {
			writer = new SAMFileWriterFactory().makeSAMWriter(header, false, new File(output));
			break;
		}
		case BAM_FLAG: {
			writer = new SAMFileWriterFactory().makeBAMWriter(header, false, new File(output));
			break;
		}
		}

		// main loop
		SAMRecord samRecord;
		SAMRecord2ReadAlignmentConverter converter = new SAMRecord2ReadAlignmentConverter();
		for (ReadAlignment readAlignment: reader) {
			samRecord = converter.backward(readAlignment);
			samRecord.setHeader(header);
			writer.addAlignment(samRecord);
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
