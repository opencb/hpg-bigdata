package org.opencb.hpg.bigdata.core.io;

import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.ga4gh.models.ReadAlignment;
import org.opencb.hpg.bigdata.core.utils.CompressionUtils;

import parquet.avro.AvroParquetOutputFormat;

public class ReadAlignment2ParquetMR {

	public static int run(String input, String output, String codecName) throws Exception {
		
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "ReadAlignment2Parquet");		
		job.setJarByClass(ReadAlignment2ParquetMR.class);

		// point to input data
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(AvroKeyInputFormat.class);

		// set the output format
		job.setOutputFormatClass(AvroParquetOutputFormat.class);
		AvroParquetOutputFormat.setOutputPath(job, new Path(output));
		AvroParquetOutputFormat.setSchema(job, ReadAlignment.SCHEMA$);
		AvroParquetOutputFormat.setCompression(job, CompressionUtils.getParquetCodec(codecName));
		AvroParquetOutputFormat.setCompressOutput(job, true);

		// set a large block size to ensure a single row group
		AvroParquetOutputFormat.setBlockSize(job, 500 * 1024 * 1024);

		job.setMapperClass(ReadAlignment2ParquetMapper.class);
		job.setNumReduceTasks(0);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
/*
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
*/