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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
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

/**
 * @author pawan
 *
 */
public class LoadBEDAndGFF2HBase extends Configured implements Tool {
    private static String rowkeySeparator = "_";
    private static String columnFamily = null;
    private Configuration conf = null;
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static class LoadBEDAndGFF2HBaseMapper extends
            Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        /*
         * (non-Javadoc)
         *
         * @see
         * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce
         * .Mapper.Context)
         */
        @Override
        protected final void setup(final Context context) throws IOException,
                InterruptedException {
            Configuration configuration = context.getConfiguration();
            columnFamily = configuration.get("familyName");
        }

        /*
         * (non-Javadoc)
         *
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
         * org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        public final void map(final LongWritable key, final Text value,
                final Context context) throws IOException, InterruptedException {

            String rowkeyStr = null;
            String[] valueArray = value.toString().split("\t");
            /*
             * create rowkey string
             */
            rowkeyStr = valueArray[0] + rowkeySeparator + valueArray[3]
                    + rowkeySeparator + valueArray[4];
            byte[] rowkeyBytes = Bytes.toBytes(rowkeyStr);
            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(
                    rowkeyBytes);
            byte[] family = Bytes.toBytes(columnFamily);

            /*
             * Create put object, add records to the column ln represents the
             * whole line
             */
            Put put = new Put(rowkeyBytes);
            put.addColumn(family, Bytes.toBytes("ln"),
                    Bytes.toBytes(value.toString()));
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
        protected final void cleanup(final Context context) throws IOException,
                InterruptedException {
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    public final int run(final String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: HBase MapReduce For BED and GFF Fromat "
                    + "<input path> <Table Name>");
            return -1;
        }

        String inputfile = args[0];
        String tablename = args[1];
        String familynameGFF = "gf";
        String familynameBED = "bd";

        conf = getConf();
        conf = HBaseConfiguration.create(HBaseConfiguration
                .addHbaseResources(conf));
        conf.set("hbase.zookeeper.quorum", "who1");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        setConf(conf);
        Job job = Job.getInstance(conf);

        job.setJarByClass(LoadBEDAndGFF2HBase.class);
        job.setJobName("Load BED and GFF data to HBase");

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(LoadBEDAndGFF2HBaseMapper.class);
        job.setNumReduceTasks(0);

        if (inputfile.endsWith(".gff")) {
            createTableIfNeeded(tablename, familynameGFF);
            job.getConfiguration().set("familyName", familynameGFF);
        } else if (inputfile.endsWith(".bed")) {
            createTableIfNeeded(tablename, familynameBED);
            job.getConfiguration().set("familyName", familynameBED);
        }

        TableMapReduceUtil.initTableReducerJob(tablename, null, job);
        FileInputFormat.setInputPaths(job, new Path(inputfile));

        return (job.waitForCompletion(true) ? 0 : 1);
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
     * @param tablename table name
     * @param columnFamily family name
     * @throws IOException EXception
     */
    public void createTableIfNeeded(String tablename, String columnFamily) throws IOException {
        TableName tname = TableName.valueOf(tablename);
        try (Connection con = ConnectionFactory.createConnection(getConf());
                Table table = con.getTable(tname);
                Admin admin = con.getAdmin()) {
            if (!exist(tname, admin)) {
                HTableDescriptor descr = new HTableDescriptor(tname);
                descr.addFamily(new HColumnDescriptor(columnFamily));
                System.out.println(String.format("Create table '%s' in hbase!",
                        tablename));
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

}
