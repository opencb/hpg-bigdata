package org.opencb.hpg.bigdata.app.cli;

import htsjdk.samtools.CRAMFileReader;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.fastq.FastqReader;
import htsjdk.samtools.util.LineReader;
import htsjdk.samtools.util.StringLineReader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.ga4gh.models.Read;
import org.ga4gh.models.ReadAlignment;
import org.ga4gh.models.Variant;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.hpg.bigdata.core.NativeSupport;
import org.opencb.hpg.bigdata.core.converters.FastqRecord2ReadConverter;
import org.opencb.hpg.bigdata.core.converters.FullVCFCodec;
import org.opencb.hpg.bigdata.core.converters.SAMRecord2ReadAlignmentConverter;
import org.opencb.hpg.bigdata.core.converters.variation.VariantAvroEncoderTask;
import org.opencb.hpg.bigdata.core.converters.variation.VariantConverterContext;
import org.opencb.hpg.bigdata.core.io.parquet.Avro2ParquetMapper;
import org.opencb.hpg.bigdata.core.io.avro.AvroFileWriter;
import org.opencb.hpg.bigdata.core.io.avro.AvroWriter;
import org.opencb.hpg.bigdata.core.utils.CompressionUtils;
import org.opencb.hpg.bigdata.core.utils.PathUtils;
import org.opencb.hpg.bigdata.core.utils.ReadUtils;
import org.opencb.hpg.bigdata.core.io.VcfBlockIterator;
import parquet.avro.AvroParquetOutputFormat;
import parquet.avro.AvroParquetWriter;
import parquet.avro.AvroSchemaConverter;
import parquet.avro.AvroWriteSupport;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

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
	private final static String CRAM_2_GA  = "cram2ga"; 
	private final static String GA_2_CRAM  = "ga2cram";
    private final static String VCF_2_GA   = "vcf2ga";

	private final static String AVRO_2_PARQUET = "avro2parquet";


	private final static String FASTQ_2_GA_DESC = "Save Fastq file as Global Alliance for Genomics and Health (ga4gh) in Avro format"; 
	private final static String GA_2_FASTQ_DESC = "Save Global Alliance for Genomics and Health (ga4gh) in Avro format as Fastq file"; 
	private final static String SAM_2_GA_DESC   = "Save SAM file as Global Alliance for Genomics and Health (ga4gh) in Avro format"; 
	private final static String GA_2_SAM_DESC   = "Save Global Alliance for Genomics and Health (ga4gh) in Avro format as SAM file";
	private final static String BAM_2_GA_DESC   = "Save BAM file as Global Alliance for Genomics and Health (ga4gh) in Avro format"; 
	private final static String GA_2_BAM_DESC   = "Save Global Alliance for Genomics and Health (ga4gh) in Avro format as BAM file";
	private final static String CRAM_2_GA_DESC  = "Save CRAM file as Global Alliance for Genomics and Health (ga4gh) in Avro format"; 
	private final static String GA_2_CRAM_DESC  = "Save Global Alliance for Genomics and Health (ga4gh) in Avro format as CRAM file";
    private final static String VCF_2_GA_DESC   = "Save VCF file as Global Alliance for Genomics and Health (ga4gh) in Avro format";

	private final static String AVRO_2_PARQUET_DESC  = "Save Avro file in Parquet format";

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

		try {
			switch (ga4ghCommandOptions.conversion) {
			case FASTQ_2_GA: {
				if (ga4ghCommandOptions.toParquet) {
					logger.info("Invalid parameter 'parquet' compression value '{}'");
					System.exit(-1);					
				}
				fastq2ga(ga4ghCommandOptions.input, ga4ghCommandOptions.output, ga4ghCommandOptions.compression);
				break;
			}
			case GA_2_FASTQ: {
				ga2fastq(ga4ghCommandOptions.input, ga4ghCommandOptions.output);
				break;
			}
			case SAM_2_GA: {
				sam2ga(ga4ghCommandOptions.input, ga4ghCommandOptions.output, ga4ghCommandOptions.compression);
				break;
			}
			case GA_2_SAM: {
				ga2sam(ga4ghCommandOptions.input, ga4ghCommandOptions.output, SAM_FLAG);
				break;
			}
			case BAM_2_GA: {
				sam2ga(ga4ghCommandOptions.input, ga4ghCommandOptions.output, ga4ghCommandOptions.compression);
				break;
			}
			case GA_2_BAM: {
				ga2sam(ga4ghCommandOptions.input, ga4ghCommandOptions.output, BAM_FLAG);
				break;
			}
			case CRAM_2_GA: {
				cram2ga(ga4ghCommandOptions.input, ga4ghCommandOptions.output, ga4ghCommandOptions.compression);
				break;
			}

			case GA_2_CRAM: {
				System.out.println("Conversion '" + ga4ghCommandOptions.conversion + "' not implemented yet.\nValid conversions are:\n" + getValidConversionString());
				/*
				ga2sam(ga4ghCommandOptions.input, ga4ghCommandOptions.output, CRAM_FLAG);
				 */
				break;
			}

            case VCF_2_GA: {
                vcf2ga(ga4ghCommandOptions.input, ga4ghCommandOptions.output, ga4ghCommandOptions.compression);
                break;
            }

			case AVRO_2_PARQUET: {
				avro2parquet(ga4ghCommandOptions.input, ga4ghCommandOptions.output);
				break;			
			}

			default: {
				logger.error("Invalid conversion {}", ga4ghCommandOptions.conversion);
				System.out.println("Invalid conversion (" + ga4ghCommandOptions.conversion + "). Valid conversions are:\n" + getValidConversionString());
				break;
			}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		logger.debug("Input file: {}", ga4ghCommandOptions.input);
	}

    private void fastq2ga(String input, String output, String codecName) throws Exception {
		// clean paths
		String in = PathUtils.clean(input);
		String out = PathUtils.clean(output);

		if (PathUtils.isHdfs(input)) {
			logger.error("Conversion '{}' with HDFS as input '{}', not implemented yet !", ga4ghCommandOptions.conversion, input);
			System.exit(-1);
		}

		// reader
		FastqReader reader = new FastqReader(new File(in));

		// writer
		OutputStream os;
		if (PathUtils.isHdfs(output)) {
			Configuration config = new Configuration();
			FileSystem hdfs = FileSystem.get(config);
			os = hdfs.create(new Path(out));
		} else {
			os = new FileOutputStream(out);
		}
		AvroWriter<Read> writer = new AvroWriter<>(Read.getClassSchema(), CompressionUtils.getAvroCodec(codecName), os);

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
		// clean paths
		String in = PathUtils.clean(input);
		String out = PathUtils.clean(output);

		if (PathUtils.isHdfs(output)) {
			logger.error("Conversion '{}' with HDFS as output '{}', not implemented yet !", ga4ghCommandOptions.conversion, output);
			System.exit(-1);
		}

		// reader
		InputStream is;
		if (PathUtils.isHdfs(input)) {
			Configuration config = new Configuration();
			FileSystem hdfs = FileSystem.get(config);
			is = hdfs.open(new Path(in));
		} else {
			is = new FileInputStream(in);
		}
		DataFileStream<Read> reader = new DataFileStream<Read>(is, new SpecificDatumReader<Read>(Read.class));

		// writer
		PrintWriter writer = new PrintWriter(new FileWriter(out));

		// main loop
		for (Read read: reader) {
			writer.write(ReadUtils.getFastqString(read));
		}

		// close
		reader.close();
		writer.close();
		is.close();
	}

	private void sam2ga(String input, String output, String codecName) throws IOException {
		// clean paths
		String in = PathUtils.clean(input);
		String out = PathUtils.clean(output);

		if (PathUtils.isHdfs(input)) {
			logger.error("Conversion '{}' with HDFS as input '{}', not implemented yet !", ga4ghCommandOptions.conversion, input);
			System.exit(-1);
		}

		if (!PathUtils.isHdfs(output) && ga4ghCommandOptions.conversion.equals(BAM_2_GA)) {
			System.loadLibrary("hpgbigdata");
			new NativeSupport().bam2ga(in, out, ga4ghCommandOptions.compression);
			return;
		}
		
		// reader (sam or bam)
		SamReader reader = SamReaderFactory.makeDefault().open(new File(in));

		// header management: saved it in a separate file
		// and writer
		OutputStream os;
		SAMFileHeader header = reader.getFileHeader();
		if (PathUtils.isHdfs(output)) {
			Configuration config = new Configuration();
			FileSystem hdfs = FileSystem.get(config);
			os = hdfs.create(new Path(out + SAM_HEADER_SUFFIX));
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
			bw.write(header.getTextHeader());
			bw.close();
			os.close();

			os = hdfs.create(new Path(out));		
		} else {
			PrintWriter pwriter = new PrintWriter(new FileWriter(output + SAM_HEADER_SUFFIX));
			pwriter.write(header.getTextHeader());
			pwriter.close();

			os = new FileOutputStream(output);
		}

		AvroWriter<ReadAlignment> writer = new AvroWriter<ReadAlignment>(ReadAlignment.getClassSchema(), CompressionUtils.getAvroCodec(codecName), os);

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
		// clean paths
		String in = PathUtils.clean(input);
		String out = PathUtils.clean(output);

		if (PathUtils.isHdfs(output)) {
			logger.error("Conversion '{}' with HDFS as output '{}', not implemented yet !", ga4ghCommandOptions.conversion, output);
			System.exit(-1);
		}

		// header management: read it from a separate file
		byte[] data = null;
		InputStream is = null;
		if (PathUtils.isHdfs(input)) {
			Path headerPath = new Path(in + SAM_HEADER_SUFFIX);
			Configuration config = new Configuration();
			FileSystem hdfs = FileSystem.get(config);
			FSDataInputStream dis = hdfs.open(headerPath);
			FileStatus status = hdfs.getFileStatus(headerPath);
			data = new byte[(int) status.getLen()];
			dis.read(data, 0, (int) status.getLen());
			dis.close();

			is = hdfs.open(new Path(in));
		} else {
			File file = new File(in + SAM_HEADER_SUFFIX);
			FileInputStream fis = new FileInputStream(file);
			data = new byte[(int) file.length()];
			fis.read(data);
			fis.close();

			is = new FileInputStream(in);			
		}

		String textHeader = new String(data);

		SAMFileHeader header = new SAMFileHeader();
		LineReader lineReader = new StringLineReader(textHeader);
		header = new SAMTextHeaderCodec().decode(lineReader, textHeader);
		
		// reader
		DataFileStream<ReadAlignment> reader = new DataFileStream<ReadAlignment>(is, new SpecificDatumReader<ReadAlignment>(ReadAlignment.class));

		// writer
		SAMFileWriter writer = null;
		OutputStream os = new FileOutputStream(new File(out));
		switch (flag) {
		case SAM_FLAG: {
			writer = new SAMFileWriterFactory().makeSAMWriter(header, false, new File(out));
			break;
		}
		case BAM_FLAG: {
			writer = new SAMFileWriterFactory().makeBAMWriter(header, false, new File(out));
			break;
		}
		case CRAM_FLAG: {
			logger.error("Conversion '{}' not implemented yet !", ga4ghCommandOptions.conversion);
			System.exit(-1);
			writer = new SAMFileWriterFactory().makeCRAMWriter(header, os, null);
			break;
		}
		}

		// main loop
		SAMRecord samRecord;
		SAMRecord2ReadAlignmentConverter converter = new SAMRecord2ReadAlignmentConverter();
		int i = 0;
		for (ReadAlignment readAlignment: reader) {
			System.out.println(readAlignment.toString());
			//System.out.println(ReadAlignmentUtils.getSamString(readAlignment));
			
			//samRecord = converter.backward(readAlignment);
			//samRecord.setHeader(header);
			//writer.addAlignment(samRecord);
		}

		// close
		reader.close();
		writer.close();
		os.close();
		is.close();
	}

	private void cram2ga(String input, String output, String codecName) throws IOException {
		logger.error("Conversion '{}' not implemented yet !", ga4ghCommandOptions.conversion);
		System.exit(-1);

		// reader
		File fi = new File(input);
		FileInputStream fis = new FileInputStream(fi);
		CRAMFileReader reader = new CRAMFileReader(fi, fis);

		// header management: saved in a separate file
		SAMFileHeader header = reader.getFileHeader();
		PrintWriter pwriter = new PrintWriter(new FileWriter(output + SAM_HEADER_SUFFIX));
		pwriter.write(header.getTextHeader());
		pwriter.close();

		// writer
		OutputStream os = new FileOutputStream(output);
		AvroWriter<ReadAlignment> writer = new AvroWriter<ReadAlignment>(ReadAlignment.getClassSchema(), CompressionUtils.getAvroCodec(codecName), os);

		// main loop
		SAMRecord2ReadAlignmentConverter converter = new SAMRecord2ReadAlignmentConverter();
		SAMRecordIterator iterator = reader.iterator();
		while (iterator.hasNext()) {
			SAMRecord samRecord = iterator.next();
			writer.write(converter.forward(samRecord));			
		}

		// close
		reader.close();
		writer.close();
		fis.close();
		os.close();
	}


    private void vcf2ga(String input, String output, String compression) throws Exception {
        if (output == null) {
            output = input;
        }

        // clean paths
        String in = PathUtils.clean(input);
        String out = PathUtils.clean(output);

        if (PathUtils.isHdfs(input)) {
            logger.error("Conversion '{}' with HDFS as input '{}', not implemented yet !", ga4ghCommandOptions.conversion, input);
            System.exit(-1);
        }

        // reader
        VcfBlockIterator iterator = new VcfBlockIterator(Paths.get(in).toFile(), new FullVCFCodec());
        DataReader<CharBuffer> reader = new DataReader<CharBuffer>() {
            @Override public List<CharBuffer> read(int size) {
                return (iterator.hasNext() ? iterator.next(size) : Collections.<CharBuffer>emptyList());
            }
            @Override public boolean close() {
                try {
                    iterator.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    return false;
                }
                return true;
            }
        };

        // writer
        OutputStream os;
        if (PathUtils.isHdfs(output)) {
            Configuration config = new Configuration();
            FileSystem hdfs = FileSystem.get(config);
            os = hdfs.create(new Path(out));
        } else {
            os = new FileOutputStream(out);
        }
        AvroFileWriter<Variant> writer = new AvroFileWriter<>(Variant.getClassSchema(), compression, os);

        // main loop
        int numTasks = 4;
        int batchSize = 1024*1024;  //Batch size in bytes
        int capacity = numTasks+1;
        VariantConverterContext variantConverterContext = new VariantConverterContext();
        ParallelTaskRunner.Config config = new ParallelTaskRunner.Config(numTasks, batchSize, capacity, false);
        ParallelTaskRunner<CharBuffer, ByteBuffer> runner =
                new ParallelTaskRunner<>(
                        reader,
                        () -> new VariantAvroEncoderTask(variantConverterContext, iterator.getHeader(), iterator.getVersion()),
                        writer, config);
        long start = System.currentTimeMillis();
        runner.run();
        System.out.println("Time " + (System.currentTimeMillis()-start)/1000.0+"s");

        // close
        iterator.close();
        writer.close();
        os.close();
    }

/*
	private void avro2parquetOK(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
*/
	private void avro2parquet(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {

		// all paths in HDFS
		Path inputPath = new Path(input);
		Path outputPath = new Path(output);

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Avro2Parquet");		
		job.setJarByClass(getClass());


		// get Avro schema
		//String str = "{\"type\":\"record\",\"name\":\"ReadAlignment\",\"namespace\":\"org.ga4gh.models\",\"doc\":\"Each read alignment describes an alignment with additional information\\nabout the fragment and the read. A read alignment object is equivalent to a\\nline in a SAM file.\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"string\"],\"doc\":\"The read alignment ID. This ID is unique within the read group this\\n  alignment belongs to. This field may not be provided by all backends.\\n  Its intended use is to make caching and UI display easier for\\n  genome browsers and other light weight clients.\"},{\"name\":\"readGroupId\",\"type\":\"string\",\"doc\":\"The ID of the read group this read belongs to.\\n  (Every read must belong to exactly one read group.)\"},{\"name\":\"fragmentName\",\"type\":\"string\",\"doc\":\"The fragment name. Equivalent to QNAME (query template name) in SAM.\"},{\"name\":\"properPlacement\",\"type\":[\"boolean\",\"null\"],\"doc\":\"The orientation and the distance between reads from the fragment are\\n  consistent with the sequencing protocol (equivalent to SAM flag 0x2)\",\"default\":false},{\"name\":\"duplicateFragment\",\"type\":[\"boolean\",\"null\"],\"doc\":\"The fragment is a PCR or optical duplicate (SAM flag 0x400)\",\"default\":false},{\"name\":\"numberReads\",\"type\":[\"null\",\"int\"],\"doc\":\"The number of reads in the fragment (extension to SAM flag 0x1)\",\"default\":null},{\"name\":\"fragmentLength\",\"type\":[\"null\",\"int\"],\"doc\":\"The observed length of the fragment, equivalent to TLEN in SAM.\",\"default\":null},{\"name\":\"readNumber\",\"type\":[\"null\",\"int\"],\"doc\":\"The read number in sequencing. 0-based and less than numberReads. This field\\n  replaces SAM flag 0x40 and 0x80.\",\"default\":null},{\"name\":\"failedVendorQualityChecks\",\"type\":[\"boolean\",\"null\"],\"doc\":\"SAM flag 0x200\",\"default\":false},{\"name\":\"alignment\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"LinearAlignment\",\"doc\":\"A linear alignment can be represented by one CIGAR string.\",\"fields\":[{\"name\":\"position\",\"type\":{\"type\":\"record\",\"name\":\"Position\",\"doc\":\"A `Position` is a side of a base pair in some already known sequence. A\\n`Position` is represented by a sequence name or ID, a base number on that\\nsequence (0-based), and a `Strand` to indicate the left or right side.\\n\\nFor example, given the sequence \\\"GTGG\\\", the `Position` on that sequence at\\noffset 1 in the forward orientation would be the left side of the T/A base pair.\\nThe base at this `Position` is \\\"T\\\". Alternately, for offset 1 in the reverse\\norientation, the `Position` would be the right side of the T/A base pair, and\\nthe base at the `Position` is \\\"A\\\".\\n\\nOffsets added to a `Position` are interpreted as reading along its strand;\\nadding to a reverse strand position actually subtracts from its `position`\\nmember.\",\"fields\":[{\"name\":\"referenceName\",\"type\":[\"null\",\"string\"],\"doc\":\"The name of the reference sequence in whatever reference set is being used.\\n  Does not generally include a \\\"chr\\\" prefix, so for example \\\"X\\\" would be used\\n  for the X chromosome.\\n\\n  If `sequenceId` is null, this must not be null.\",\"default\":null},{\"name\":\"sequenceId\",\"type\":[\"null\",\"string\"],\"doc\":\"The ID of the sequence on which the `Position` is located. This may be a\\n  `Reference` sequence, or a novel piece of sequence associated with a\\n  `VariantSet`.\\n\\n  If `referenceName` is null, this must not be null.\\n\\n  If the server supports the \\\"graph\\\" mode, this must not be null.\",\"default\":null},{\"name\":\"position\",\"type\":\"long\",\"doc\":\"The 0-based offset from the start of the forward strand for that sequence.\\n  Genomic positions are non-negative integers less than sequence length.\"},{\"name\":\"strand\",\"type\":{\"type\":\"enum\",\"name\":\"Strand\",\"doc\":\"Indicates the DNA strand associate for some data item.\\n* `POS_STRAND`:  The postive (+) strand.\\n* `NEG_STRAND`: The negative (-) strand.\\n* `NO_STRAND`: Strand-independent data or data where the strand can not be determined.\",\"symbols\":[\"POS_STRAND\",\"NEG_STRAND\",\"NO_STRAND\"]},\"doc\":\"Strand the position is associated with. `POS_STRAND` represents the forward\\n  strand, or equivalently the left side of a base, and `NEG_STRAND` represents\\n  the reverse strand, or equivalently the right side of a base.\"}]},\"doc\":\"The position of this alignment.\"},{\"name\":\"mappingQuality\",\"type\":[\"null\",\"int\"],\"doc\":\"The mapping quality of this alignment. Represents how likely\\n  the read maps to this position as opposed to other locations.\",\"default\":null},{\"name\":\"cigar\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"CigarUnit\",\"doc\":\"A structure for an instance of a CIGAR operation.\",\"fields\":[{\"name\":\"operation\",\"type\":{\"type\":\"enum\",\"name\":\"CigarOperation\",\"doc\":\"An enum for the different types of CIGAR alignment operations that exist.\\nUsed wherever CIGAR alignments are used. The different enumerated values\\nhave the following usage:\\n\\n* `ALIGNMENT_MATCH`: An alignment match indicates that a sequence can be\\n  aligned to the reference without evidence of an INDEL. Unlike the\\n  `SEQUENCE_MATCH` and `SEQUENCE_MISMATCH` operators, the `ALIGNMENT_MATCH`\\n  operator does not indicate whether the reference and read sequences are an\\n  exact match. This operator is equivalent to SAM's `M`.\\n* `INSERT`: The insert operator indicates that the read contains evidence of\\n  bases being inserted into the reference. This operator is equivalent to\\n  SAM's `I`.\\n* `DELETE`: The delete operator indicates that the read contains evidence of\\n  bases being deleted from the reference. This operator is equivalent to\\n  SAM's `D`.\\n* `SKIP`: The skip operator indicates that this read skips a long segment of\\n  the reference, but the bases have not been deleted. This operator is\\n  commonly used when working with RNA-seq data, where reads may skip long\\n  segments of the reference between exons. This operator is equivalent to\\n  SAM's 'N'.\\n* `CLIP_SOFT`: The soft clip operator indicates that bases at the start/end\\n  of a read have not been considered during alignment. This may occur if the\\n  majority of a read maps, except for low quality bases at the start/end of\\n  a read. This operator is equivalent to SAM's 'S'. Bases that are soft clipped\\n  will still be stored in the read.\\n* `CLIP_HARD`: The hard clip operator indicates that bases at the start/end of\\n  a read have been omitted from this alignment. This may occur if this linear\\n  alignment is part of a chimeric alignment, or if the read has been trimmed\\n  (e.g., during error correction, or to trim poly-A tails for RNA-seq). This\\n  operator is equivalent to SAM's 'H'.\\n* `PAD`: The pad operator indicates that there is padding in an alignment.\\n  This operator is equivalent to SAM's 'P'.\\n* `SEQUENCE_MATCH`: This operator indicates that this portion of the aligned\\n  sequence exactly matches the reference (e.g., all bases are equal to the\\n  reference bases). This operator is equivalent to SAM's '='.\\n* `SEQUENCE_MISMATCH`: This operator indicates that this portion of the\\n  aligned sequence is an alignment match to the reference, but a sequence\\n  mismatch (e.g., the bases are not equal to the reference). This can\\n  indicate a SNP or a read error. This operator is equivalent to SAM's 'X'.\",\"symbols\":[\"ALIGNMENT_MATCH\",\"INSERT\",\"DELETE\",\"SKIP\",\"CLIP_SOFT\",\"CLIP_HARD\",\"PAD\",\"SEQUENCE_MATCH\",\"SEQUENCE_MISMATCH\"]},\"doc\":\"The operation type.\"},{\"name\":\"operationLength\",\"type\":\"long\",\"doc\":\"The number of bases that the operation runs for.\"},{\"name\":\"referenceSequence\",\"type\":[\"null\",\"string\"],\"doc\":\"`referenceSequence` is only used at mismatches (`SEQUENCE_MISMATCH`)\\n  and deletions (`DELETE`). Filling this field replaces the MD tag.\\n  If the relevant information is not available, leave this field as `null`.\",\"default\":null}]}},\"doc\":\"Represents the local alignment of this sequence (alignment matches, indels, etc)\\n  versus the reference.\",\"default\":[]}]},{\"type\":\"record\",\"name\":\"GraphAlignment\",\"doc\":\"A string-to-reference-graph alignment can be represented by one CIGAR string and\\none `Path` through multiple `Reference`s, against which the CIGAR string is\\ninterpreted.\\n\\nNote that `Path`s in `GraphAlignment`s are restricted to visiting `Reference`s\\nand following reference adjacencies. If a read needs to be aligned to sequences\\nthat are not present in a `ReferenceSet`, it needs to be aligned to a new\\n`ReferenceSet` with those sequences. If a read needs to follow adjacencies that\\nare not present in the `ReferenceSet` it's being aligned to, it should be\\nrepresented as a \\\"chimeric\\\" alignment, and should use multiple `ReadAlignment`s\\nand the supplementaryAlignment flag instead of a single `GraphAlignment`.\\n\\nSome especially large deletions could be represented just as well as a large\\ndeletion in the CIGAR string, or as a chimeric alignment.\",\"fields\":[{\"name\":\"path\",\"type\":{\"type\":\"record\",\"name\":\"Path\",\"doc\":\"A `Path` is an ordered list of `Segment`s. In general any contiguous path\\nthrough a sequence graph, with no novel adjacencies, can be represented by a\\n`Path`.\",\"fields\":[{\"name\":\"segments\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Segment\",\"doc\":\"A `Segment` is a range on a sequence, possibly including the joins at the\\nsequence's ends. It does not include base data. (The bases for a sequence are\\navailable through the `getSequenceBases()` API call.)\\n\\nIn the sequence \\\"GTGG\\\", the segment starting at index 1 on the forward strand\\nwith length 2 is the \\\"TG\\\" on the forward strand. The length-2 segment starting\\nat index 1 on the reverse strand is \\\"AC\\\", corresponding to the first two base\\npairs of the sequence, or the last two bases of the reverse complement.\",\"fields\":[{\"name\":\"start\",\"type\":\"Position\",\"doc\":\"The sequence ID and start index of this `Segment`. This base is always\\n  included in the segment, regardless of orientation.\"},{\"name\":\"length\",\"type\":\"long\",\"doc\":\"The length of this `Segment`'s sequence. If `start` is on the forward strand,\\n  the `Segment` contains the range [`start.position`, `start.position` +\\n  `length`). If `start` is on the reverse strand, the `Segment` contains the\\n  range (`start.position` - `length`, `start.position`]. This is equivalent to\\n  starting from the side indicated by `start`, and traversing through that base\\n  out to the specified length.\"},{\"name\":\"startJoin\",\"type\":[\"null\",\"Position\"],\"doc\":\"Start and end `Position`s where this `Segment` attaches to other sequences.\\n  Note that the segmentId for start and end might not be the same. The\\n  `Segment`s covering the sequences onto which this `Segment` is joined are\\n  called its \\\"parents\\\", while this segment is a \\\"child\\\".\\n\\n  Joins may occur on the outer sides of the terminal bases in a sequence: the\\n  left side of the base at index 0, and the right side of the base with maximum\\n  index. These are the \\\"terminal sides\\\" of the sequence. `startJoin` is the join\\n  on the side indicated by `start`, and may only be set if that side is a\\n  terminal side. Similarly, `endJoin` is the join on the opposite side of the\\n  piece of sequence selected by the segment, and may only be set if that side is\\n  a terminal side. The value of `startJoin` or `endJoin`, if set, is the side to\\n  which the corresponding side of this `Sequence` is connected.\",\"default\":null},{\"name\":\"endJoin\",\"type\":[\"null\",\"Position\"],\"default\":null}]}},\"doc\":\"We require that one of each consecutive pair of `Segment`s in a `Path` be\\n  joined onto the other. `Segment`s appear in the order in which they occur when\\n  walking the path from one end to the other.\",\"default\":[]}]},\"doc\":\"The `Path` against which the read is aligned\"},{\"name\":\"mappingQuality\",\"type\":[\"null\",\"int\"],\"doc\":\"The mapping quality of this alignment. Represents how likely\\n  the read maps to this position as opposed to other locations.\",\"default\":null},{\"name\":\"cigar\",\"type\":{\"type\":\"array\",\"items\":\"CigarUnit\"},\"doc\":\"Represents the local alignment of this sequence (alignment matches, indels,\\n  etc) versus the `Path`.\",\"default\":[]}]}],\"doc\":\"The alignment for this alignment record. This field will be\\n  null if the read is unmapped.\\n\\n  If an API server supports \\\"classic\\\" mode, it must not return `GraphAlignment`\\n  objects here. If the API server supports the \\\"graph\\\" mode and does not support\\n  the \\\"classic\\\" mode, it must not return `LinearAlignment` objects here.\",\"default\":null},{\"name\":\"secondaryAlignment\",\"type\":[\"boolean\",\"null\"],\"doc\":\"Whether this alignment is secondary. Equivalent to SAM flag 0x100.\\n  A secondary alignment represents an alternative to the primary alignment\\n  for this read. Aligners may return secondary alignments if a read can map\\n  ambiguously to multiple coordinates in the genome.\\n\\n  By convention, each read has one and only one alignment where both\\n  secondaryAlignment and supplementaryAlignment are false.\",\"default\":false},{\"name\":\"supplementaryAlignment\",\"type\":[\"boolean\",\"null\"],\"doc\":\"Whether this alignment is supplementary. Equivalent to SAM flag 0x800.\\n  Supplementary alignments are used in the representation of a chimeric\\n  alignment, which follows nonreference adjacencies not describable as indels.\\n  In a chimeric alignment, a read is split into multiple alignments that\\n  may map to different reference contigs. The first alignment in the read will\\n  be designated as the representative alignment; the remaining alignments will\\n  be designated as supplementary alignments. These alignments may have different\\n  mapping quality scores.\\n\\n  In each alignment in a chimeric alignment, the read will be hard clipped. The\\n  `alignedSequence` and `alignedQuality` fields in the alignment record will\\n  only represent the bases for its respective alignment.\",\"default\":false},{\"name\":\"alignedSequence\",\"type\":[\"null\",\"string\"],\"doc\":\"The bases of the read sequence contained in this alignment record.\\n  `alignedSequence` and `alignedQuality` may be shorter than the full read sequence\\n  and quality. This will occur if the alignment is part of a chimeric alignment,\\n  or if the read was trimmed. When this occurs, the CIGAR for this read will\\n  begin/end with a hard clip operator that will indicate the length of the excised sequence.\",\"default\":null},{\"name\":\"alignedQuality\",\"type\":{\"type\":\"array\",\"items\":\"int\"},\"doc\":\"The quality of the read sequence contained in this alignment record.\\n  `alignedSequence` and `alignedQuality` may be shorter than the full read sequence\\n  and quality. This will occur if the alignment is part of a chimeric alignment,\\n  or if the read was trimmed. When this occurs, the CIGAR for this read will\\n  begin/end with a hard clip operator that will indicate the length of the excised sequence.\",\"default\":[]},{\"name\":\"nextMatePosition\",\"type\":[\"null\",\"Position\"],\"doc\":\"The mapping of the primary alignment of the `(readNumber+1)%numberReads`\\n  read in the fragment. It replaces mate position and mate strand in SAM.\",\"default\":null},{\"name\":\"info\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"array\",\"items\":\"string\"}},\"doc\":\"A map of additional read alignment information.\",\"default\":{}}]}"; 
		//Schema schema = new org.apache.avro.Schema.Parser().parse(str); //ReadAlignment.getClassSchema();
		//Schema schema = Read.getClassSchema(); //ReadAlignment.getClassSchema();
		Schema schema = ReadAlignment.getClassSchema();
		//System.out.println(new AvroSchemaConverter().convert(schema).toString());

		// point to input data
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(AvroKeyInputFormat.class);

		// set the output format
		job.setOutputFormatClass(AvroParquetOutputFormat.class);
		AvroParquetOutputFormat.setOutputPath(job, outputPath);
		AvroParquetOutputFormat.setSchema(job, schema);
		AvroParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
		AvroParquetOutputFormat.setCompressOutput(job, true);

		// set a large block size to ensure a single row group.  see discussion
		AvroParquetOutputFormat.setBlockSize(job, 500 * 1024 * 1024);

		job.setMapperClass(Avro2ParquetMapper.class);
		job.setNumReduceTasks(0);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	private void avro2parquetLocal(String input, String output) throws IOException {		
		// generate the corresponding parquet schema
		MessageType parquetSchema = new AvroSchemaConverter().convert(ReadAlignment.getClassSchema());

		// create a WriteSupport object to serialize your Avro objects
		AvroWriteSupport writeSupport = new AvroWriteSupport(parquetSchema, ReadAlignment.getClassSchema());

		// set parquet file block size and page size values
		int blockSize = 256 * 1024 * 1024;
		int pageSize = 64 * 1024;


		// the ParquetWriter object that will consume Avro GenericRecords
		ParquetWriter parquetWriter = new AvroParquetWriter(new Path(output),
				ReadAlignment.getClassSchema(), CompressionCodecName.GZIP, blockSize, pageSize);
		//ReadAlignment.getClassSchema(), CompressionCodecName.SNAPPY, blockSize, pageSize);
		// reader
		InputStream is = new FileInputStream(input);
		DataFileStream<ReadAlignment> reader = new DataFileStream<ReadAlignment>(is, new SpecificDatumReader<ReadAlignment>(ReadAlignment.class));

		// main loop
		for (ReadAlignment readAlignment: reader) {
			parquetWriter.write(readAlignment);
		}

		parquetWriter.close();
	}

	private String getValidConversionString() {
		String res = new String();
		res += "\t- " + FASTQ_2_GA + "\t" + FASTQ_2_GA_DESC + "\n";
		res += "\t- " + GA_2_FASTQ + "\t" + GA_2_FASTQ_DESC + "\n";
		res += "\t- " + SAM_2_GA + "\t" + SAM_2_GA_DESC + "\n";
		res += "\t- " + GA_2_SAM + "\t" + GA_2_SAM_DESC + "\n";
		res += "\t- " + BAM_2_GA + "\t" + BAM_2_GA_DESC + "\n";
		res += "\t- " + GA_2_BAM + "\t" + GA_2_BAM_DESC + "\n";
		res += "\t- " + CRAM_2_GA + "\t" + CRAM_2_GA_DESC + "\n";
		res += "\t- " + VCF_2_GA + "\t" + VCF_2_GA_DESC+ "\n";
		//res += "\t- " + GA_2_CRAM + "\t" + GA_2_CRAM_DESC + "\n";

		res += "\n";
		res += "\t- " + AVRO_2_PARQUET + "\t" + AVRO_2_PARQUET_DESC + "\n";

		return res;

	}
}
