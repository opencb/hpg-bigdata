package org.opencb.hpg.bigdata.tools.spark.hbase.avro;

import com.cloudera.spark.hbase.JavaHBaseContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by pawan on 10/03/16.
 */
public class VariantSparkHBaseGet {

    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_CYAN = "\u001B[36m";

    public static void main(String[] args) {
        System.out.println(ANSI_CYAN + " length :: " + args.length + ANSI_RESET);
        System.out.println(ANSI_CYAN + " args[0] :: " + args[0] + ANSI_RESET);
        System.out.println(ANSI_CYAN + " args[1] :: " + args[1] + ANSI_RESET);
        System.out.println(ANSI_CYAN + " args[2] :: " + args[2] + ANSI_RESET);

        if (args.length == 0) {
            System.out.println(ANSI_CYAN + "JavaHBaseBulkGetExample  {master} {tableName}, {rowkey}" + ANSI_RESET);
        }

        String master = args[0];
        String tableName = args[1];
        String rowkey = args[2];

        JavaSparkContext jsc = new JavaSparkContext(master, "JavaHBaseBulkGetExample");
        jsc.addJar("spark.jar");

        List<byte[]> list = new ArrayList<>();
        list.add(Bytes.toBytes(rowkey));
//        list.add(Bytes.toBytes("2"));
//        list.add(Bytes.toBytes("3"));
//        list.add(Bytes.toBytes("4"));
//        list.add(Bytes.toBytes("5"));

        JavaRDD<byte[]> rdd = jsc.parallelize(list);

        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

        JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
        JavaRDD<String> rdd2 =  hbaseContext.bulkGet(tableName, 2, rdd, new GetFunction(), new ResultFunction());
        rdd2.collect().forEach(System.out::println);

        jsc.close();
    }

    public static class GetFunction implements Function<byte[], Get> {
        private static final long serialVersionUID = 1L;
        public Get call(byte[] v) throws Exception {
            return new Get(v);
        }
    }

    public static class ResultFunction implements Function<Result, String> {

        private static final long serialVersionUID = 1L;

        public String call(Result result) throws Exception {
            Iterator<KeyValue> it = result.list().iterator();
            StringBuilder b = new StringBuilder();
            b.append(Bytes.toString(result.getRow()) + ":");

            while (it.hasNext()) {
                KeyValue kv = it.next();
                String q = Bytes.toString(kv.getQualifier());
                if (q.equals("counter")) {
                    b.append("(" + Bytes.toString(kv.getQualifier()) + "," + Bytes.toLong(kv.getValue()) + ")");
                } else {
                    b.append("(" + Bytes.toString(kv.getQualifier()) + "," + Bytes.toString(kv.getValue()) + ")");
                }
            }
            return b.toString();
        }
    }
}
