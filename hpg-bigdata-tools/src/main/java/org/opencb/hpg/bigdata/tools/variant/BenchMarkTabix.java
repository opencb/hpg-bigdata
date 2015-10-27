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
package org.opencb.hpg.bigdata.tools.variant;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.opencb.biodata.models.variant.Variant;

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.tools.variant.converter.VariantTabix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Kalyan
 *
 */
public class BenchMarkTabix extends
Mapper<AvroKey<VariantAvro>, NullWritable, ImmutableBytesWritable, Put>  implements Tool {

    private final byte[] COLUMN_FAMILY = Bytes.toBytes("cf");
    private final String ROWKEY_SEPARATOR = "_";
    private final char PATH_SEPARATORCHAR = '/';
    private final String PATH_SEPARATORSTRING = "/";
    private final String FILE_SUFFIX = ".";
    private final String avroSuffix=".avro";
    private final String filePrefix="file";
    private final String META="meta";
    private Configuration conf;
    private String columnValue;

    @Override
    protected void setup(
            Context context)
                    throws IOException, InterruptedException {
        //this value will be used for the entry in Meta
        columnValue=conf.get("columnNameValue");
        super.setup(context);
    }

    /**
     * Takes the VCF Avro file as input and gives the Variant Object.
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(AvroKey<VariantAvro> key, NullWritable value, Context context)
            throws IOException, InterruptedException {
        VariantAvro variantAvro = key.datum();
        byte[] id = Bytes.toBytes(constructRowKey(variantAvro));
        VariantTabix variantAvroToVcfRecord= new VariantTabix();
        Put put = new Put(id);
        put.addColumn(COLUMN_FAMILY, Bytes.toBytes("c1"),
                variantAvroToVcfRecord.convert(constructVariant(key.datum())).toByteArray());
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(id);
        context.write(rowKey, put);
    }

    public Variant constructVariant(VariantAvro variantAvro) {
        Variant variant=new Variant();
        variant.setChromosome(variantAvro.getChromosome());
        variant.setStart(variantAvro.getStart());
        variant.setEnd(variantAvro.getEnd());
        variant.setReference(variantAvro.getReference());
        variant.setAlternate(variantAvro.getAlternate());
        List<StudyEntry> variantSourceEntryList = new ArrayList<>();
        StudyEntry variantSourceEntry;
        List<org.opencb.biodata.models.variant.avro.StudyEntry> variantSourceEntryAvroList= variantAvro.getStudies();
        for (org.opencb.biodata.models.variant.avro.StudyEntry variantSourceEntryAvro :variantSourceEntryAvroList) {
            variantSourceEntry=new StudyEntry(variantSourceEntryAvro);
            variantSourceEntryList.add(variantSourceEntry);
        }
        variant.setStudies(variantSourceEntryList);
        variant.setIds(variantAvro.getIds());
        variant.setHgvs(variantAvro.getHgvs());
        variant.setAnnotation(variantAvro.getAnnotation());
        return variant;
    }
    /**
     * Rowkey construction with chromosome,Position,Reference,Alternate.
     * @param variant variant object from the mapper
     * @return returns the rowkey as string
     */
    public String constructRowKey(VariantAvro variant) {
        int pos= variant.getStart();
        int position = 0;
        if ((variant.getLength()) == 1) {
            position = pos;
        } else {
            position = pos - 1;

        }
        String chromosome = variant.getChromosome();
        StringBuilder builder = new StringBuilder(chromosome);
        builder.append(ROWKEY_SEPARATOR);
        builder.append(position);
        builder.append(ROWKEY_SEPARATOR);
        builder.append(variant.getReference());
        builder.append(ROWKEY_SEPARATOR);
        builder.append(variant.getAlternate());
        return builder.toString();
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: HBase MapReduce For VCF " + "<input path> <Table Name> <Host Name> "
                    + "<HDFS Path> <Load Type>");
            return -1;
        }
        //Getting the parameters from the command line
        String inputFile = args[0];
        String tableName = args[1];
        String hostName = args[2];
        String hdfsPath = args[3];
        String loadType = args[4];

        String fileName = null;
        String fileNameWithOutExt = null;
        //conf = getConf();
        //setting the configuration for zookeeper
        conf = new Configuration();
        conf = HBaseConfiguration.addHbaseResources(conf);
        conf.set("hbase.zookeeper.quorum", hostName);
        //This file name will be used in defining the final hdfs path which is required for the job
        // fileName = inputFile.substring(inputFile.lastIndexOf(PATH_SEPARATORCHAR) + 1, inputFile.length());
        //this will be used for the meta entry
        // fileNameWithOutExt=fileName.substring(1, fileName.lastIndexOf(FILE_SUFFIX));
        //getting the columnValueFromHbase from row key meta and qualifier filename
        //  columnValue = getColumnNameValue(fileNameWithOutExt, tableName , loadType);
        // conf.set("columnNameValue", columnValue);
        conf.set("columnName", "c1");

        FileSystem fs = FileSystem.get(conf);

        /*checks the file type , if the file type is local then call the converter and
          then move the generated avro to hdfs.
          if the file type is local and then ends with avro then copy the avro file to hdfs
          if the file type is hdfs and then this bolock will do nothing.*/
        Job job = Job.getInstance(conf);
        AvroJob.setInputKeySchema(job, VariantAvro.getClassSchema());
        FileInputFormat.setInputPaths(job, new Path(inputFile));
        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setJarByClass(BenchMarkTabix.class);
        job.setJobName("VCF Tabix HBase");
        job.setMapperClass(BenchMarkTabix.class);
        TableMapReduceUtil.initTableReducerJob(tableName, null, job);
        job.setNumReduceTasks(0);
        int jobStatus;
        if (job.waitForCompletion(true)) {
            jobStatus=0;
        } else {
            jobStatus=1;
        }
        return jobStatus;
    }
}
