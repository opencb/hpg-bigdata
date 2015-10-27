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

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * @author pawan
 *
 */
public class LoadBEDAndGFF2HBase extends Configured implements Tool {

    private static String rowkeySeparator = "_";
    private static String columnFamily = null;
    private static String fileName = null;
    //private static String tableName = null;
    private Configuration conf = null;
    private static String columnValue = null;;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static class LoadBEDAndGFF2HBaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        /*
         * (non-Javadoc)
         *
         * @see
         * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce
         * .Mapper.Context)
         */
        @Override
        protected final void setup(final Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();

            columnFamily = configuration.get("familyName");
            fileName = configuration.get("filename");
            //tableName = configuration.get("tablename");
            columnValue = configuration.get("columnNameValue");
        }

        /*
         * (non-Javadoc)
         *
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
         * org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        public final void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {

            /*
             * Create separate rowkey to identify if file has been uploaded
             * before
             */
            //addMetadata(fileName, tableName, columnFamily);
            String rowkeyStr = null;
            String[] valueArray = value.toString().split("\t");
            /*
             * create rowkey string
             */
            rowkeyStr = valueArray[0] + rowkeySeparator + valueArray[3] + rowkeySeparator + valueArray[4];
            byte[] rowkeyBytes = Bytes.toBytes(rowkeyStr);
            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rowkeyBytes);
            byte[] family = Bytes.toBytes(columnFamily);

            /*
             * Create put object, add records to the column ln represents the
             * whole line
             */
            Put put = new Put(rowkeyBytes);
            put.addColumn(family, Bytes.toBytes(columnValue), Bytes.toBytes(value.toString()));
            context.write(rowKey, put);
        }

        /*
         * (non-Javadoc)
         *
         * @see
         * org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce
         * .Mapper.Context)
         */
        @Override
        protected final void cleanup(final Context context) throws IOException, InterruptedException {
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    public final int run(final String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: HBase MapReduce For BED and GFF Fromat " + "<input path> <Table Name> <Host Name> "
                    + "<HDFS Path> <Load Type>");
            return -1;
        }
        String inputfile = args[0];
        String tablename = args[1];
        String hostName = args[2];
        String hdfsDirPath = args[3];
        String loadType = args[4];

        //conf = getConf();
        conf = new Configuration();
        conf = HBaseConfiguration.create(HBaseConfiguration.addHbaseResources(conf));
        conf.set("hbase.zookeeper.quorum", hostName);
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        setConf(conf);

        String inputFilePath = getInputFilePath(inputfile, hdfsDirPath);

        String fileName = null;
        String fileNameWithOutExt = null;

        fileName = inputfile.substring(inputfile.lastIndexOf('/'), inputfile.length());
        fileNameWithOutExt = fileName.substring(1, fileName.lastIndexOf("."));

        String columnValue = getColumnNameValue(fileNameWithOutExt, tablename, loadType);
        conf.set("columnNameValue", columnValue);
        conf.set("columnName", fileNameWithOutExt);

        Job job = Job.getInstance(conf);
        job.setJarByClass(LoadBEDAndGFF2HBase.class);
        job.setJobName("Load BED and GFF data to HBase");
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(LoadBEDAndGFF2HBaseMapper.class);
        job.setNumReduceTasks(0);
        job.getConfiguration().set("filename", inputFilePath);
        job.getConfiguration().set("tablename", tablename);
        createTableIfNeeded(tablename, getColumnFamilyName(fileName));
        job.getConfiguration().set("familyName", getColumnFamilyName(fileName));
        TableMapReduceUtil.initTableReducerJob(tablename, null, job);
        FileInputFormat.setInputPaths(job, new Path(inputFilePath));
        int jobStatus;
        if (job.waitForCompletion(true)) {
            registerNewFile(fileNameWithOutExt, tablename, columnValue);
            jobStatus = 0;
        } else {
            jobStatus = 1;
        }
        return jobStatus;
        //return (job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * @param fileNameWithoutExt
     * @param tableName
     * @param loadType
     * @return
     * @throws IOException
     * @throws JSONException jsonException
     */
    private String getColumnNameValue(String fileNameWithoutExt, String tableName, String loadType) throws IOException, JSONException {

        TableName tname = TableName.valueOf(tableName);
        Connection con = ConnectionFactory.createConnection(getConf());
        Table table = con.getTable(tname);
        Result row = table.get(new Get(Bytes.toBytes("meta")));

        if (row.isEmpty()) {
            String returnValue = "c1";
            return returnValue;
        } else {

            byte[] value = row.getValue(Bytes.toBytes(getColumnFamilyName(fileName)), Bytes.toBytes(fileNameWithoutExt));

            String valueStr = Bytes.toString(value);
            if (valueStr == null) {
                valueStr = "c" + (row.rawCells().length + 1);
            } else {
                JSONObject obj = new JSONObject(valueStr);
                valueStr = obj.get("columnName").toString();
                if (loadType.equals("forceDelete")) {
                    List<Delete> deletes = new ArrayList<Delete>();
                    Scan s = new Scan();
                    s.addColumn(Bytes.toBytes(getColumnFamilyName(fileName)), value);
                    ResultScanner scanner = table.getScanner(s);
                    try {
                        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                            Delete delete = new Delete(Bytes.toBytes(rr.toString()));
                            delete.addColumn(Bytes.toBytes(getColumnFamilyName(fileName)), value);
                            deletes.add(delete);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    table.delete(deletes);
                }
                table.close();
                return valueStr;
            }
            return valueStr;
        }

    }

    /**
     * @param args
     *            get the argument
     * @throws Exception
     *             if not find the configuration
     */

    public static void main(final String[] args) throws Exception {
        int res = 0;
        try {
            res = ToolRunner.run(new LoadBEDAndGFF2HBase(), args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(res);
        }
    }


    /**
     * @param filename
     * @return
     */
    private String getColumnFamilyName(String filename) {
        String columnFamilyName = null;
        if (fileName.endsWith(".bed")) {
            columnFamilyName = "bd";
        } else if (fileName.endsWith(".gff")) {
            columnFamilyName = "gf";
        }
        return columnFamilyName;

    }

    /**
     * @param tablename
     *            table name
     * @param columnFamily
     *            family name
     * @throws IOException
     *             EXception
     */
    public void createTableIfNeeded(String tablename, String columnFamily) throws IOException {
        TableName tname = TableName.valueOf(tablename);
        try (Connection con = ConnectionFactory.createConnection(getConf());
                Table table = con.getTable(tname);
                Admin admin = con.getAdmin()) {
            if (!exist(tname, admin)) {
                HTableDescriptor descr = new HTableDescriptor(tname);
                descr.addFamily(new HColumnDescriptor(columnFamily));
                System.out.println(String.format("Create table '%s' in hbase!", tablename));
                admin.createTable(descr);
            }
        }
    }

    /**
     * @param tname
     * @param admin
     * @return
     * @throws IOException
     *             IOException
     */
    private boolean exist(TableName tname, Admin admin) throws IOException {
        for (TableName tn : admin.listTableNames()) {
            if (tn.equals(tname)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param filename
     * @param tableName
     * @param columnValue
     * @throws IOException Exception
     */
    private void registerNewFile(String filename, String tableName, String columnValue) throws IOException {
        JSONObject obj = new JSONObject();
        try {
            obj.put("columnName", columnValue);
            obj.put("headerInformation", "headerInformation");
            TableName tname = TableName.valueOf(tableName);
            Connection con = ConnectionFactory.createConnection(getConf());
            Table table = con.getTable(tname);
            Put p = new Put(Bytes.toBytes("meta"));
            p.addColumn(Bytes.toBytes(getColumnFamilyName(fileName)), Bytes.toBytes(filename),
                    Bytes.toBytes(obj.toString()));
            table.put(p);
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param inputFile
     * @param hdfsPath
     * @return
     * @throws IOException Exception
     */
    private String  getInputFilePath(String inputFile, String hdfsPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        fileName = inputFile.substring(inputFile.lastIndexOf('/') + 1, inputFile.length());
        try {
            if (inputFile.startsWith("file")) {

                java.nio.file.Path sourceFilepath = Paths.get(inputFile.substring(6, inputFile.length()));
                fs.copyFromLocalFile(new Path(sourceFilepath.toString()), new Path(hdfsPath));
                if (inputFile.endsWith(".bed")) {
                    inputFile = hdfsPath + "/" + fileName.substring(0, fileName.lastIndexOf(".")) + ".bed";
                } else if (inputFile.endsWith("gff")) {
                    inputFile = hdfsPath + "/" + fileName.substring(0, fileName.lastIndexOf(".")) + ".gff";
                }
            } else {
                inputFile = hdfsPath + "/" + fileName;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            fs.close();
        }
        return inputFile;
    }
}
