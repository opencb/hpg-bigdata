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
import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.ga4gh.models.Call;
import org.ga4gh.models.Variant;
import org.opencb.hpg.bigdata.core.utils.HBaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class Variant2HbaseMR extends Mapper<AvroKey<Variant>, NullWritable, ImmutableBytesWritable, Put> {

    private static final String VARIANT_2_HBASE_EXPAND_REGIONS = "VARIANT_2_HBASE.EXPAND_REGIONS";
    private static final String VARIANT_2_HBASE_NON_VAR = "VARIANT_2_HBASE.NON_VARIANT";

    public static final byte[] COLUMN_FAMILY = Bytes.toBytes("d");

    private static final Logger LOG = LoggerFactory.getLogger(Variant2HbaseMR.class);
    private Configuration config;
    private boolean expandRegions = false;
    private boolean nonVariant = false;

    public Variant2HbaseMR() {
        super();
    }

    public static Logger getLog() {
        return LOG;
    }

    public void setExpandRegions(boolean expandRegions) {
        this.expandRegions = expandRegions;
    }

    public boolean isExpandRegions() {
        return expandRegions;
    }

    public boolean isNonVariant() {
        return nonVariant;
    }

    public void setNonVariant(boolean nonVariant) {
        this.nonVariant = nonVariant;
    }

    @Override
    protected void setup(
            Mapper<AvroKey<Variant>, NullWritable, ImmutableBytesWritable, Put>.Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        setExpandRegions(conf.getBoolean(VARIANT_2_HBASE_EXPAND_REGIONS, isExpandRegions()));
        setNonVariant(conf.getBoolean(VARIANT_2_HBASE_NON_VAR, isNonVariant()));
        super.setup(context);
    }

    @Override
    protected void map(AvroKey<Variant> key, NullWritable value, Mapper<AvroKey<Variant>, NullWritable,
            ImmutableBytesWritable, Put>.Context context)
            throws IOException, InterruptedException {
        Variant variant = key.datum();

        if (isReference(variant) && this.isNonVariant()) { // is just reference or non-call
            String refplaceholder = "#"; // TODO require lookup service to expand
            Long start = variant.getStart();
            Long endPos = start + 1;
            List<Call> calls = variant.getCalls();
            boolean nocall = calls.isEmpty();
            if (isExpandRegions()) {
                context.getCounter("VCF", "REG_EXPAND" + (nocall ? "_NOCALL" : "")).increment(1);
                Map<CharSequence, List<CharSequence>> info = variant.getInfo();
                List<CharSequence> endLst = info.get("END"); // Get End position

                if (null == endLst || endLst.isEmpty()) {
                    // Region of size 1
                    context.getCounter("VCF", "REF_END_EMPTY" + (nocall ? "_NOCALL" : "")).increment(1);
                } else {
                    String endStr = endLst.get(0).toString();
                    endPos = Long.valueOf(endStr);
                }
            }
            String counterName = "REG_EXPAND_CNT" + (nocall ? "_NOCALL" : "");
            context.getCounter("VCF", counterName).increment((endPos - start));
            if (!nocall) { // only if calls
                for (long pos = start; pos < endPos; ++pos) {
                    // For each position -> store
                    String idStr = HBaseUtils.buildRefernceStorageId(variant.getReferenceName(), pos, refplaceholder);
                    store(context, calls, idStr);
                }
            }
        } else { // is a variant (not just coverage info)
            int altCnt = variant.getAlternateBases().size();
            if (altCnt > 1) {
                context.getCounter("VCF", "biallelic_COUNT").increment(1);
                return; // skip biallelic cases
            }
            List<Call> calls = variant.getCalls();
            if (null == calls || calls.isEmpty()) {
                context.getCounter("VCF", "NO_CALL_COUNT").increment(1);
                return; // skip SV
            }
            int altIdx = 0;
            CharSequence altBases = "-";
            if (altCnt > 0) {
                altBases = variant.getAlternateBases().get(altIdx);
            }
            CharSequence refBases = variant.getReferenceBases();
            if (altBases.length() >= HBaseUtils.SV_THRESHOLD || refBases.length() >= HBaseUtils.SV_THRESHOLD) {
                context.getCounter("VCF", "SV_COUNT").increment(1);
                return; // skip SV
            }
            String idStr = HBaseUtils.buildStorageId(variant.getReferenceName(), variant.getStart(), refBases, altBases);

            store(context, calls, idStr);

            /* Ignore fields */
//          List<CharSequence> ids = v.getAlleleIds(); // graph mode -> not supported

            /* TODO fields - fine for first implementation*/
//            v.getInfo()
//            v.getNames()
//            v.getEnd();

        }
    }

    private void store(
            Mapper<AvroKey<Variant>, NullWritable, ImmutableBytesWritable, Put>.Context context,
            List<Call> calls, String idStr) throws IOException,
            InterruptedException {
        byte[] id = Bytes.toBytes(idStr);
        Put put = new Put(id);
        for (Call call : calls) {
            addEntry(put, call);
        }
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(id);

        /* Submit data to HBase */
        context.write(rowKey, put);
    }

    private boolean isReference(Variant variant) {
        return null == variant.getAlternateBases() || variant.getAlternateBases().isEmpty();
    }

    private void addEntry(Put put, Call call) {
        CharSequence id = call.getCallSetId();
        String idStr = id.toString();
        /* other possibility
         * id = call.getCallSetName()
         */

        // TODO check what happens in case of > 1 alt base
        put.addColumn(
                COLUMN_FAMILY,
                Bytes.toBytes(idStr),
                Bytes.toBytes(call.toString())
        );   // json
    }

    public void setConf(Configuration conf) {
        this.config = conf;
    }

    public Configuration getConf() {
        return this.config;
    }

    public static class Builder {
        private URI uri;
        private String inputfile;
        private boolean expand = false;
        private boolean non_var = false;

        public Builder(String inputfile, URI uri) {
            this.inputfile = inputfile;
            this.uri = uri;
        }

        public Builder setUri(URI uri) {
            this.uri = uri;
            return this;
        }

        public Builder setInputfile(String inputfile) {
            this.inputfile = inputfile;
            return this;
        }

        public Builder setExpand(boolean expand) {
            this.expand = expand;
            return this;
        }

        public Builder setNonVar(boolean nonVar) {
            this.non_var = nonVar;
            return this;
        }

        public Job build(boolean createTableIfNeeded) throws IOException {

/*  INPUT file  */
            String inputfile = this.inputfile;

/*  SERVER details  */
            String server = null;
            Integer port = 60000;
            String tablename = null;

            if (null == uri) {
                throw new IllegalArgumentException("No Server output specified!");
            }

            server = uri.getHost();
            if (StringUtils.isBlank(server)) {
                throw new IllegalArgumentException("No Server host name specified in URI: " + uri);
            }

            if (uri.getPort() > 0) { // if port is specified
                port = uri.getPort();
            }
            String master = String.join(":", server, port.toString());

/*  TABLE details  */
            if (StringUtils.isBlank(uri.getPath()) || StringUtils.equals(uri.getPath().trim(), "/")) {
                throw new IllegalArgumentException("No Table name specified in URI: " + uri);
            }
            // Extract table name from Path
            tablename = uri.getPath();
            tablename = tablename.startsWith("/") ? tablename.substring(1) : tablename; // Remove leading /

            getLog().info(String.format("Loading data into server '%s' using table '%s' ", master, tablename));

/*  CONFIG  */
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", server);
            conf.set("hbase.master", master);

            // SET additional parameters
            conf.setBoolean(VARIANT_2_HBASE_EXPAND_REGIONS, this.expand);
            conf.setBoolean(VARIANT_2_HBASE_NON_VAR, this.non_var);

            // HBase
            conf = HBaseConfiguration.addHbaseResources(conf);

/*  JOB setup  */
            Class<Variant2HbaseMR> clazz = Variant2HbaseMR.class;
            Job job = Job.getInstance(conf, clazz.getName());
            job.setJarByClass(clazz);

            // input
            AvroJob.setInputKeySchema(job, Variant.getClassSchema());
            FileInputFormat.setInputPaths(job, new Path(inputfile));
            job.setInputFormatClass(AvroKeyInputFormat.class);

            job.setNumReduceTasks(0);

            // output
            TableMapReduceUtil.initTableReducerJob(tablename, null, job);

            // mapper
            job.setMapperClass(Variant2HbaseMR.class);

/*  TABLE check  */
            if (createTableIfNeeded) {
                // create table if needed
                createTableIfNeeded(tablename, conf);
            }
            return job;
        }
    }

    /**
     * Create HBase table if needed.
     *
     * @param tablename     HBase table name
     * @throws IOException throws {@link IOException} from creating a connection / table
     */
    public void createTableIfNeeded(String tablename) throws IOException {
        createTableIfNeeded(tablename, getConf());
    }

    /**
     * Create default HBase table layout with one column family using {@link COLUMN_FAMILY}.
     *
     * @param tablename     HBase table name
     * @param configuration HBase configuration
     * @throws IOException throws {@link IOException} from creating a connection / table
     **/
    public static void createTableIfNeeded(String tablename, Configuration configuration) throws IOException {
        TableName tname = TableName.valueOf(tablename);
        try (
                Connection con = ConnectionFactory.createConnection(configuration);
                Table table = con.getTable(tname);
                Admin admin = con.getAdmin();
        ) {
            if (!exist(tname, admin)) {
                HTableDescriptor descr = new HTableDescriptor(tname);
                descr.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
                getLog().info(String.format("Create table '%s' in hbase!", tablename));
                admin.createTable(descr);
            }
        }
    }

    private static boolean exist(TableName tname, Admin admin) throws IOException {
        for (TableName tn : admin.listTableNames()) {
            if (tn.equals(tname)) {
                return true;
            }
        }
        return false;
    }
}
