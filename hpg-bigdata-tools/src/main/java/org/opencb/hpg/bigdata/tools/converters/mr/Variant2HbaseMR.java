/**
 * 
 */
package org.opencb.hpg.bigdata.tools.converters.mr;

import java.io.IOException;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.ga4gh.models.Call;
import org.ga4gh.models.Variant;
import org.opencb.commons.utils.CryptoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class Variant2HbaseMR extends Mapper<AvroKey<Variant>, NullWritable, ImmutableBytesWritable, Put> implements Tool {
    public final static byte[] COLUMN_FAMILY = Bytes.toBytes("d");
    private final static String ROWKEY_SEPARATOR = "_";
    private static final int SV_THRESHOLD = 50; // TODO update as needed 
	
    private final Logger log;
	private Configuration config;

	public Variant2HbaseMR() {
		super();
		log = LoggerFactory.getLogger(this.getClass().toString());
	}
	
	public Logger getLog() {
		return log;
	}
	
	@Override
	protected void setup(
			Mapper<AvroKey<Variant>, NullWritable, ImmutableBytesWritable, Put>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
	}
	
	@Override
	protected void map(
			AvroKey<Variant> key,
			NullWritable value,
			Mapper<AvroKey<Variant>, NullWritable, ImmutableBytesWritable, Put>.Context context)
			throws IOException, InterruptedException {
		Variant variant = key.datum();
			
		if(!isReference(variant)){ // is a variant (not just coverage info)
	        byte[] id = Bytes.toBytes(buildStorageId(variant));
	        
			Put put = new Put(id);

	        /* Ignore fields */
//	      List<CharSequence> ids = v.getAlleleIds(); // graph mode -> not supported

	        /* TODO fields - fine for first implementation*/        
//	        v.getInfo() 
//	        v.getNames()
//	        v.getEnd();
	        
	        List<Call> calls = variant.getCalls();
	        for(Call call : calls){
	        	addEntry(put,call);
	        }
	        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(id);
	        
	        /* Submit data to HBase */
//			context.write(rowKey, put);
		}		
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
        put.addColumn(
        		COLUMN_FAMILY, 
        		Bytes.toBytes(id.toString()), 
        		Bytes.toBytes(call.toString()));        			
	}

	public String buildStorageId(Variant v) {
        CharSequence chr = v.getReferenceName(); // TODO check for chr at chromosome name and remove it (maybe expect it to be done before.
		StringBuilder builder = new StringBuilder(chr); 
        builder.append(ROWKEY_SEPARATOR);
        builder.append(String.format("%012d", v.getStart()));
        builder.append(ROWKEY_SEPARATOR);
        
        if (v.getReferenceBases().length() < SV_THRESHOLD) {
            builder.append(v.getReferenceBases());
        } else {
            builder.append(new String(CryptoUtils.encryptSha1(v.getReferenceBases().toString())));
        }

        builder.append(ROWKEY_SEPARATOR);
        
        if(v.getAlternateBases().size() > 1)
        	throw new NotImplementedException("More than one alternate for same position not yet supported!!! for position " + builder.toString());
        
        if(v.getAlternateBases().size() == 1){
        	CharSequence ab = v.getAlternateBases().get(0);
            if (ab.length() < SV_THRESHOLD) {
                builder.append(ab);
            } else {
                builder.append(new String(CryptoUtils.encryptSha1(ab.toString())));
            }        	
        }
        
        return builder.toString();
    }

	@Override
	public void setConf(Configuration conf) {
		this.config = conf;
	}

	@Override
	public Configuration getConf() {
		return this.config;
	}
	
	public static int run(String[] args,String other) throws Exception{
		Configuration conf = new Configuration();
		String tablename = "test_table";
		String inputfile = null;
		String output = null;
		for(int i = 0; i < args.length; ++i){
			if(args[i] == "-t")
				tablename = args[++i];
			if(args[i] == "-i")
				inputfile = args[++i];
			if(args[i] == "-o")
				output = args[++i];
		}
		Job job = Job.getInstance(conf, "Variant2HBase");
		job.setJarByClass(Variant2HbaseMR.class);

		// input
		AvroJob.setInputKeySchema(job, Variant.getClassSchema());
		FileInputFormat.setInputPaths(job, new Path(inputfile));
		job.setInputFormatClass(AvroKeyInputFormat.class);
		
		job.setNumReduceTasks(0); 
	    
		// mapper
		job.setMapperClass(Variant2HbaseMR.class);

		return (job.waitForCompletion(true) ? 0 : 1);		
	}

	@Override
	public int run(String[] args) throws Exception {
		getLog().info(String.format("Configuration: %s ", getConf()));
        setConf(new Configuration());
		String tablename = "test_table";
		String inputfile = null;
		String output = null;
		for(int i = 0; i < args.length; ++i){
			if(args[i] == "-t")
				tablename = args[++i];
			if(args[i] == "-i")
				inputfile = args[++i];
			if(args[i] == "-o")
				output = args[++i];
		}

//	    setConf(HBaseConfiguration.addHbaseResources(getConf()));

	    Job job = Job.getInstance(getConf());
	    job.setJobName(this.getClass().getName() + "_" + tablename);
		job.setJarByClass(this.getClass());

		// input
		AvroJob.setInputKeySchema(job, Variant.getClassSchema());
		FileInputFormat.setInputPaths(job, new Path(inputfile));
		job.setInputFormatClass(AvroKeyInputFormat.class);

		// output -> Hbase
//		TableMapReduceUtil.initTableReducerJob(tablename, null, job);
		job.setNumReduceTasks(0); // Write to table directory
		if(StringUtils.isNotBlank(output)){
			Configuration conf = getConf();
			conf.set("hbase.zookeeper.quorum", output);
			conf.set("hbase.master", output+":60000");
			setConf(conf);
		}
	    
		// mapper
		job.setMapperClass(Variant2HbaseMR.class);
		
		// create Table if needed
//		createTableIfNeeded(tablename);
		long start = System.currentTimeMillis();
		boolean completed = job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		getLog().info(String.format("Loading run for %s ms!", (end-start)));
		return completed?0:1;
	}

	/**
	 * Create HBase table if needed
	 * 
	 * @param tablename
	 * @throws IOException
	 */
	public void createTableIfNeeded(String tablename) throws IOException {
		TableName tname = TableName.valueOf(tablename);
		try(
			Connection con = ConnectionFactory.createConnection(getConf());
			Table table = con.getTable(tname);
			Admin admin = con.getAdmin();
			){
			if(!exist(tname, admin)){
				HTableDescriptor descr = new HTableDescriptor(tname);
				descr.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
				getLog().info(String.format("Create table '%s' in hbase!", tablename));
				admin.createTable(descr);
			}
		}
	}

	private boolean exist(TableName tname, Admin admin) throws IOException {
		for (TableName tn : admin.listTableNames()) {
			if(tn.equals(tname)){
				return true;
			}
		}
		return false;
	}
}
