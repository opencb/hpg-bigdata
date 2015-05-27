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

import java.io.BufferedInputStream;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.ga4gh.models.ReadAlignment;
import org.ga4gh.models.Variant;
import org.opencb.biodata.models.sequence.Read;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.hpg.bigdata.core.NativeSupport;
import org.opencb.hpg.bigdata.core.converters.FastqRecord2ReadConverter;
import org.opencb.hpg.bigdata.core.converters.FullVcfCodec;
import org.opencb.hpg.bigdata.core.converters.SAMRecord2ReadAlignmentConverter;
import org.opencb.hpg.bigdata.core.converters.variation.VariantAvroEncoderTask;
import org.opencb.hpg.bigdata.core.converters.variation.VariantConverterContext;
import org.opencb.hpg.bigdata.tools.converters.mr.Bam2AvroMR;
import org.opencb.hpg.bigdata.tools.converters.mr.Variant2HbaseMR;
import org.opencb.hpg.bigdata.core.io.VcfBlockIterator;
import org.opencb.hpg.bigdata.core.io.avro.AvroFileWriter;
import org.opencb.hpg.bigdata.core.io.avro.AvroWriter;
import org.opencb.hpg.bigdata.tools.converters.mr.Fastq2AvroMR;
import org.opencb.hpg.bigdata.tools.converters.mr.Vcf2AvroMR;
import org.opencb.hpg.bigdata.tools.io.parquet.ParquetConverter;
import org.opencb.hpg.bigdata.tools.io.parquet.ParquetMR;
import org.opencb.hpg.bigdata.core.utils.AvroUtils;
import org.opencb.hpg.bigdata.core.utils.PathUtils;
import org.opencb.hpg.bigdata.core.utils.ReadUtils;

import static org.opencb.hpg.bigdata.tools.converters.mr.Fastq2AvroMR.*;

/**
 * Created by imedina on 16/03/15.
 */
public class ConvertCommandExecutor extends CommandExecutor {

    public enum Conversion {
        FASTQ_2_AVRO ("fastq2avro", "Save Fastq file as Global Alliance for Genomics and Health (ga4gh) in Avro format"),
        AVRO_2_FASTQ ("avro2fastq", "Save Global Alliance for Genomics and Health (ga4gh) in Avro format as Fastq file"),
        SAM_2_AVRO ("sam2avro", "Save SAM file as Global Alliance for Genomics and Health (ga4gh) in Avro format"),
        AVRO_2_SAM ("avro2sam", "Save Global Alliance for Genomics and Health (ga4gh) in Avro format as SAM file"),
        BAM_2_AVRO ("bam2avro", "Save BAM file as Global Alliance for Genomics and Health (ga4gh) in Avro format"),
        AVRO_2_BAM ("avro2bam", "Save Global Alliance for Genomics and Health (ga4gh) in Avro format as BAM file"),
        CRAM_2_AVRO ("cram2avro", "Save CRAM file as Global Alliance for Genomics and Health (ga4gh) in Avro format"),
        AVRO_2_CRAM ("avro2cram", "Save Global Alliance for Genomics and Health (ga4gh) in Avro format as CRAM file"),
        VCF_2_AVRO ("vcf2avro", "Save VCF file as Global Alliance for Genomics and Health (ga4gh) in Avro format"),
        VARIANT_2_HBASE ("variant2hbase", "Load ga4gh avro variant file into HBase"),
        AVRO_2_PARQUET ("avro2parquet", "Save Avro file in Parquet format"),
        ;

        final private String name;
        final private String description;
        final static Map<String, Conversion> names = new HashMap<>();

        Conversion(String name, String description) {
            this.name = name;
            this.description = description;
        }

        static public Conversion fromName(String name) {
            if (names.isEmpty()) {
                for (Conversion conversion : Conversion.values()) {
                    names.put(conversion.toString(), conversion);
                }
            }
            return names.get(name);
        }

        @Override
        public String toString() {
            return name;
        }

        static public String getValidConversionString() {
            String res = "";

            Conversion[] valid = new Conversion[]{
                    Conversion.FASTQ_2_AVRO,
                    Conversion.AVRO_2_FASTQ,
                    Conversion.SAM_2_AVRO,
                    Conversion.AVRO_2_SAM,
                    Conversion.BAM_2_AVRO,
                    Conversion.AVRO_2_BAM,
                    Conversion.CRAM_2_AVRO,
                    Conversion.VCF_2_AVRO,
                    Conversion.VARIANT_2_HBASE
//                Conversion.AVRO_2_PARQUET,
            };

            for (Conversion conversion : valid) {
                res += "\t- " + conversion.name + "\t" + conversion.description + "\n";
            }

            return res;

        }
    }
	public final static String SAM_HEADER_SUFFIX = ".header";

	private final static int SAM_FLAG  = 0;
	private final static int BAM_FLAG  = 1;
	private final static int CRAM_FLAG = 2;

	private CliOptionsParser.ConvertCommandOptions convertCommandOptions;

	public ConvertCommandExecutor(CliOptionsParser.ConvertCommandOptions convertCommandOptions) {
		super(convertCommandOptions.commonOptions.logLevel, convertCommandOptions.commonOptions.verbose,
				convertCommandOptions.commonOptions.conf);

		this.convertCommandOptions = convertCommandOptions;
	}


	/**
	 * Parse specific 'convert' command options
	 */
	public void execute() {
		logger.info("Executing {} CLI options", "convert");

		try {
			switch (convertCommandOptions.conversion) {
			case FASTQ_2_AVRO: {
				if (convertCommandOptions.toParquet) {
					logger.info("Invalid parameter 'parquet' compression value '{}'");
					System.exit(-1);					
				}
				fastq2avro(convertCommandOptions.input, convertCommandOptions.output, convertCommandOptions.compression);
				break;
			}
			case AVRO_2_FASTQ: {
				avro2fastq(convertCommandOptions.input, convertCommandOptions.output);
				break;
			}
			case SAM_2_AVRO: {
				sam2avro(convertCommandOptions.input, convertCommandOptions.output, convertCommandOptions.compression);
				break;
			}
			case AVRO_2_SAM: {
				avro2sam(convertCommandOptions.input, convertCommandOptions.output, SAM_FLAG);
				break;
			}
			case BAM_2_AVRO: {
				sam2avro(convertCommandOptions.input, convertCommandOptions.output, convertCommandOptions.compression);
				break;
			}
			case AVRO_2_BAM: {
				avro2sam(convertCommandOptions.input, convertCommandOptions.output, BAM_FLAG);
				break;
			}
			case CRAM_2_AVRO: {
				cram2avro(convertCommandOptions.input, convertCommandOptions.output, convertCommandOptions.compression);
				break;
			}

			case AVRO_2_CRAM: {
				System.out.println("Conversion '" + convertCommandOptions.conversion + "' not implemented yet.\nValid conversions are:\n" + Conversion.getValidConversionString());
				/*
				avro2sam(ga4ghCommandOptions.input, ga4ghCommandOptions.output, CRAM_FLAG);
				 */
				break;
			}

            case VCF_2_AVRO: {
                vcf2avro(convertCommandOptions.input, convertCommandOptions.output, convertCommandOptions.compression);
                break;
            }
            case VARIANT_2_HBASE: {
                variant2hbase(convertCommandOptions.input, convertCommandOptions.output);
                break;
            }

//			case AVRO_2_PARQUET: {
//				avro2parquet(ga4ghCommandOptions.input, ga4ghCommandOptions.output);
//				break;			
//			}

			default: {
				logger.error("Invalid conversion {}", convertCommandOptions.conversion);
				System.out.println("Invalid conversion (" + convertCommandOptions.conversion + "). Valid conversions are:\n" + Conversion.getValidConversionString());
				System.exit(-1);
			}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		logger.debug("Input file: {}", convertCommandOptions.input);
	}


    private void fastq2avro(String input, String output, String codecName) throws Exception {
		// clean paths
		String in = PathUtils.clean(input);
		String out = PathUtils.clean(output);

		if (PathUtils.isHdfs(input)) {

			if (!PathUtils.isHdfs(output)) {
				logger.error("To run command sam2avro with HDFS input file, then import output files '{}' must be stored in the HDFS/Haddop too.", output);
				System.exit(-1);
			}

			try {
				Fastq2AvroMR.run(in, out, codecName);
			} catch (Exception e) {
				e.printStackTrace();
			}

			return;
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
		AvroWriter<Read> writer = new AvroWriter<>(Read.getClassSchema(), AvroUtils.getCodec(codecName), os);

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

	private void avro2fastq(String input, String output) throws IOException {
		// clean paths
		String in = PathUtils.clean(input);
		String out = PathUtils.clean(output);

		if (PathUtils.isHdfs(output)) {
			logger.error("Conversion '{}' with HDFS as output '{}', not implemented yet !", convertCommandOptions.conversion, output);
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

	private void sam2avro(String input, String output, String codecName) throws IOException {
		// clean paths
		String in = PathUtils.clean(input);
		String out = PathUtils.clean(output);

		if (PathUtils.isHdfs(input)) {
			
			if (!PathUtils.isHdfs(output)) {
				logger.error("To run command sam2avro with HDFS input file, then output files '{}' must be stored in the HDFS/Haddop too.", output);
				System.exit(-1);
			}

			try {
				Bam2AvroMR.run(in, out, codecName);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			return;
		} 
		
		if (!PathUtils.isHdfs(output) && convertCommandOptions.conversion.equals(Conversion.BAM_2_AVRO)) {
            System.out.println("Loading library hpgbigdata...");
            System.out.println("\tjava.libary.path = " + System.getProperty("java.library.path"));
            System.loadLibrary("hpgbigdata");
            System.out.println("...done!");
			new NativeSupport().bam2ga(in, out, convertCommandOptions.compression == null ? "snappy" : convertCommandOptions.compression);
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

		AvroWriter<ReadAlignment> writer = new AvroWriter<ReadAlignment>(ReadAlignment.getClassSchema(), AvroUtils.getCodec(codecName), os);

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

	private void avro2sam(String input, String output, int flag) throws IOException {
		// clean paths
		String in = PathUtils.clean(input);
		String out = PathUtils.clean(output);

		if (PathUtils.isHdfs(output)) {
			logger.error("Conversion '{}' with HDFS as output '{}', not implemented yet !", convertCommandOptions.conversion, output);
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
			logger.error("Conversion '{}' not implemented yet !", convertCommandOptions.conversion);
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

	private void cram2avro(String input, String output, String codecName) throws IOException {
		logger.error("Conversion '{}' not implemented yet !", convertCommandOptions.conversion);
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
		AvroWriter<ReadAlignment> writer = new AvroWriter<ReadAlignment>(ReadAlignment.getClassSchema(), AvroUtils.getCodec(codecName), os);

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


    private void variant2hbase(String input, String output) throws Exception {
		Variant2HbaseMR mr = new Variant2HbaseMR();
		List<String> args = new ArrayList<String>(Arrays.asList(new String[]{"-i",input,"-t","VariantLoad"}));
		if(StringUtils.isNotBlank(output)){
			args.add("-o");
			args.add(output);
		}
		int run = ToolRunner.run(mr, args.toArray(new String[0]));
		if(run != 0)
			throw new IllegalStateException(String.format("Variant 2 HBase finished with %s !", run));
	}

	private void vcf2avro(String input, String output, String compression) throws Exception {
//        String output = convertCommandOptions.output;
//        String input = convertCommandOptions.input;
//        String compression = convertCommandOptions.compression;
        if (output == null) {
            output = input;
        }

        // clean paths
        String in = PathUtils.clean(input);
        String out = PathUtils.clean(output);

        if (convertCommandOptions.toParquet) {
            logger.info("Transform {} to parquet", input);

            if (PathUtils.isHdfs(input)) {
                new ParquetMR(Variant.getClassSchema()).run(in, out, compression);
            } else {
                new ParquetConverter<Variant>(Variant.getClassSchema()).toParquet(new FileInputStream(in), out);
            }

        } else {
            if (PathUtils.isHdfs(input)) {

                try {
                    Vcf2AvroMR.run(in, out, compression);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } else {
                // reader
                VcfBlockIterator iterator =
                        StringUtils.equals("-", in) ?
                                new VcfBlockIterator(new BufferedInputStream(System.in), new FullVcfCodec())
                                : new VcfBlockIterator(Paths.get(in).toFile(), new FullVcfCodec());
                DataReader<CharBuffer> reader = new DataReader<CharBuffer>() {
                    @Override
                    public List<CharBuffer> read(int size) {
                        return (iterator.hasNext() ? iterator.next(size) : Collections.<CharBuffer>emptyList());
                    }

                    @Override
                    public boolean close() {
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
                int numTasks = Integer.getInteger("convert.vcf2avro.parallel", 4);
                int batchSize = 1024 * 1024;  //Batch size in bytes
                int capacity = numTasks + 1;
                VariantConverterContext variantConverterContext = new VariantConverterContext();
                ParallelTaskRunner.Config config = new ParallelTaskRunner.Config(numTasks, batchSize, capacity, false);
                ParallelTaskRunner<CharBuffer, ByteBuffer> runner =
                        new ParallelTaskRunner<>(
                                reader,
                                () -> new VariantAvroEncoderTask(variantConverterContext, iterator.getHeader(), iterator.getVersion()),
                                writer, config);
                long start = System.currentTimeMillis();
                runner.run();
                System.out.println("Time " + (System.currentTimeMillis() - start) / 1000.0 + "s");

                // close
                iterator.close();
                writer.close();
                os.close();

            }
        }
    }
}




















