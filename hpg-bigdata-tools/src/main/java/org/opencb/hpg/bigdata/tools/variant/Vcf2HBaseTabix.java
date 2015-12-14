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
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.tools.variant.converter.VariantToProtoVcfRecord;

import java.nio.file.Paths;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Kalyan
 *
 */
public class Vcf2HBaseTabix extends
Mapper<AvroKey<VariantAvro>, NullWritable, ImmutableBytesWritable, Put> implements Tool {

    public static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf");
    private static final String ROWKEY_SEPARATOR = "_";
    private static final char PATH_SEPARATORCHAR = '/';
    private static final String PATH_SEPARATORSTRING = "/";
    private static final String FILE_SUFFIX = ".";
    private final String avroSuffix=".avro";
    private final String filePrefix="file";
    private final String META="meta";
    private Configuration conf;
    private String columnValue;

    @Override
    protected void setup(
            Mapper<AvroKey<VariantAvro>, NullWritable, ImmutableBytesWritable, Put>.Context context)
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
        VariantToProtoVcfRecord variantAvroToVcfRecord = new VariantToProtoVcfRecord();
        Put put = new Put(id);
        //TODO this will be removed once the changes in the ProtoConverter is ready
        put.addColumn(
                COLUMN_FAMILY, Bytes.toBytes(columnValue),
                variantAvro.getChromosome().getBytes());
        //TODO need to uncomment as Matthias needs to change his Proto Converter as
        // it is taking Variant currently(needs to take VariantAvro)
        //                    put.addColumn(
        //                            COLUMN_FAMILY, Bytes.toBytes(columnValue),
        //                            variantAvroToVcfRecord.convert(variantAvro).toByteArray());
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(id);
        context.write(rowKey, put);
    }

    /**
     * Rowkey construction with chromosome,Position,Reference,Alternate.
     * @param variant variant object from the mapper
     * @return returns the rowkey as string
     */
    public static String constructRowKey(VariantAvro variant) {
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

        String fileName=null;
        String fileNameWithOutExt=null;
        //conf = getConf();
        //setting the configuration for zookeeper
        conf = new Configuration();
        conf = HBaseConfiguration.addHbaseResources(conf);
        conf.set("hbase.zookeeper.quorum", hostName);
        //This file name will be used in defining the final hdfs path which is required for the job
        fileName = inputFile.substring(inputFile.lastIndexOf(PATH_SEPARATORCHAR) + 1, inputFile.length());
        //this will be used for the meta entry
        fileNameWithOutExt=fileName.substring(1, fileName.lastIndexOf(FILE_SUFFIX));
        //getting the columnValueFromHbase from row key meta and qualifier filename
        columnValue = getColumnNameValue(fileNameWithOutExt, tableName , loadType);
        conf.set("columnNameValue", columnValue);
        conf.set("columnName", fileNameWithOutExt);

        FileSystem fs = FileSystem.get(conf);

        /*checks the file type , if the file type is local then call the converter and
            //then move the generated avro to hdfs.
            //if the file type is local and then ends with avro then copy the avro file to hdfs
             if the file type is hdfs and then this bolock will do nothing.*/

        try {
            if (inputFile.startsWith(filePrefix)) {
                if (fileName.endsWith(avroSuffix)) {
                    fs.copyFromLocalFile(new Path(inputFile), new Path(hdfsPath));
                    inputFile = hdfsPath + PATH_SEPARATORSTRING  + fileName;
                } else {
                    java.nio.file.Path sourceFilepath = Paths.get(inputFile.substring(6, inputFile.length()));
                    String destinationFilePathString=inputFile.substring(6, inputFile.lastIndexOf(FILE_SUFFIX)) + avroSuffix;
                    java.nio.file.Path destFilePath = Paths.get(destinationFilePathString);
//                  VariantContextToVariantConverter variantContextToVariantConverter = new VariantContextToVariantConverter("", fileName);
//                  variantContextToVariantConverter.convert(variantContext).readVCFFile(sourceFilepath, destFilePath);
                    fs.moveFromLocalFile(new Path(destFilePath.toString()), new Path(hdfsPath));
                    inputFile = hdfsPath + PATH_SEPARATORSTRING + fileName.substring(0, fileName.lastIndexOf(FILE_SUFFIX)) + avroSuffix;
                }
            } else {
                if (!fileName.endsWith(avroSuffix)) {
                    System.err.println("************************************************************");
                    System.err.println("If the file is vcf.gz then it should be available in local dir..");
                    System.err.println("**************************************************************");
                    return -1;
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            fs.close();
        }

        Job job = Job.getInstance(conf);
        AvroJob.setInputKeySchema(job, VariantAvro.getClassSchema());
        FileInputFormat.setInputPaths(job, new Path(inputFile));
        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setJarByClass(Vcf2HBaseTabix.class);
        job.setJobName("VCF Tabix HBase");
        job.setMapperClass(Vcf2HBaseTabix.class);
        TableMapReduceUtil.initTableReducerJob(tableName, null, job);
        job.setNumReduceTasks(0);
        createTableIfNeeded(tableName);
        int jobStatus;
        if (job.waitForCompletion(true)) {
            registerNewFile(fileNameWithOutExt, tableName, columnValue);
            jobStatus=0;
        } else {
            jobStatus=1;
        }
        return jobStatus;
    }

    /**
     * This will be called once the file is loaded in Hbase so that this can be made an entry.
     * @param fileName FileName
     * @param tableName TableName
     * @param columnValue columnName
     * @throws IOException
     * @throws JSONException
     */
    private void registerNewFile(String fileName, String tableName, String columnValue) throws IOException, JSONException {
        TableName tablename = TableName.valueOf(tableName);
        Connection con = ConnectionFactory.createConnection(getConf());
        Table table = con.getTable(tablename);
        JSONObject obj=new JSONObject();
        obj.put("columnName", columnValue);
        // TODO header information
        obj.put("headerInformation", "headerInformation");
        Put put = new Put(Bytes.toBytes(META));
        put.addColumn(COLUMN_FAMILY, Bytes.toBytes(fileName),
                Bytes.toBytes(obj.toString()));
        table.put(put);
    }

    /**
     * Get the columnName from meta id no entry then returns c1.
     * @param fileName fileName
     * @param tableName table name
     * @param loadType load type
     * @return
     * @throws IOException
     * @throws JSONException
     */
    private String getColumnNameValue(String fileName, String tableName, String loadType) throws IOException, JSONException {
        TableName tname = TableName.valueOf(tableName);
        Connection con = ConnectionFactory.createConnection(getConf());
        Table table = con.getTable(tname);
        Result row = table.get(new Get(Bytes.toBytes(META)));
        if (row.isEmpty()) {
            String returnValue="c1";
            return returnValue;
        } else {
            byte[] value = row.getValue(COLUMN_FAMILY, Bytes
                    .toBytes(fileName));
            String valueStr = Bytes.toString(value);
            if (valueStr  == null) {
                valueStr="c" + (row.rawCells().length + 1);
            } else {
                JSONObject obj = new JSONObject(valueStr);
                valueStr=obj.get("columnName").toString();
                if (loadType.equals("forceDelete")) {
                    List<Delete> deletes = new ArrayList<Delete>();
                    Scan s = new Scan();
                    s.addColumn(COLUMN_FAMILY, valueStr.getBytes());
                    ResultScanner scanner = table.getScanner(s);
                    try {
                        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                            Delete delete = new Delete(rr.getRow());
                            delete.addColumn(COLUMN_FAMILY, valueStr.getBytes());
                            deletes.add(delete);
                        }

                    } finally {
                        table.close();
                        scanner.close();

                    }
                    return valueStr;
                }
            }
            return valueStr;
        }

    }

    /**
     * Create HBase table if needed.
     *
     * @param tablename name of the table
     * @throws IOException IOexception propagated
     */
    public void createTableIfNeeded(String tablename) throws IOException {
        TableName tname = TableName.valueOf(tablename);
        try (
                Connection con = ConnectionFactory.createConnection(getConf());
                Table table = con.getTable(tname);
                Admin admin = con.getAdmin();
                ) {
            if (!exist(tname, admin)) {
                HTableDescriptor descr = new HTableDescriptor(tname);
                descr.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
                admin.createTable(descr);
            }
        }
    }

    private boolean exist(TableName tname, Admin admin) throws IOException {
        for (TableName tn : admin.listTableNames()) {
            if (tn.equals(tname)) {
                return true;
            }
        }
        return false;
    }
}
