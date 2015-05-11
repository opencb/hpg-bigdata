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

import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.opencb.hpg.bigdata.core.io.Bam2GaMR;
import org.opencb.hpg.bigdata.core.io.VcfBlockIterator;
import org.opencb.hpg.bigdata.core.io.avro.AvroFileWriter;
import org.opencb.hpg.bigdata.core.io.avro.AvroWriter;
import org.opencb.hpg.bigdata.core.utils.CompressionUtils;
import org.opencb.hpg.bigdata.core.utils.PathUtils;
import org.opencb.hpg.bigdata.core.utils.ReadUtils;

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

//			case AVRO_2_PARQUET: {
//				avro2parquet(ga4ghCommandOptions.input, ga4ghCommandOptions.output);
//				break;			
//			}

			default: {
				logger.error("Invalid conversion {}", ga4ghCommandOptions.conversion);
				System.out.println("Invalid conversion (" + ga4ghCommandOptions.conversion + "). Valid conversions are:\n" + getValidConversionString());
				System.exit(-1);
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
			
			if (!PathUtils.isHdfs(output)) {
				logger.error("To run command sam2ga with HDFS input file, then output files '{}' must be stored in the HDFS/Haddop too.", output);
				System.exit(-1);
			}

			try {
				Bam2GaMR.run(in, out, codecName);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			return;
		} 
		
		if (!PathUtils.isHdfs(output) && ga4ghCommandOptions.conversion.equals(BAM_2_GA)) {
			System.loadLibrary("hpgbigdata");
			new NativeSupport().bam2ga(in, out, ga4ghCommandOptions.compression == null ? "snappy" : ga4ghCommandOptions.compression);
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
		for (ReadAlignment readAlignment: reader) {			
			samRecord = converter.backward(readAlignment);
			samRecord.setHeader(header);			
			writer.addAlignment(samRecord);
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
        int numTasks = Integer.getInteger("ga4gh.vcf2ga.parallel", 4);
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
