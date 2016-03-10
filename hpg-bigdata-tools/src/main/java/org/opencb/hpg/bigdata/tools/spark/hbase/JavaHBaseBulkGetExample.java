package org.opencb.hpg.bigdata.tools.spark.hbase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
import com.cloudera.spark.hbase.JavaHBaseContext;

public class JavaHBaseBulkGetExample {
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_CYAN = "\u001B[36m";

    public static void main(String[] args) {
        System.out.println(ANSI_CYAN + " length :: " + args.length + ANSI_RESET);
        System.out.println(ANSI_CYAN + " master :: " + args[0] + ANSI_RESET);
        System.out.println(ANSI_CYAN + " table :: " + args[1] + ANSI_RESET);
        if (args.length == 0) {
            System.out.println(ANSI_CYAN + "JavaHBaseBulkGetExample  {master} {tableName}" + ANSI_RESET);
        }

        String master = args[0];
        String tableName = args[1];

        JavaSparkContext jsc = new JavaSparkContext(master, "JavaHBaseBulkGetExample");
        jsc.addJar("spark.jar");

        List<byte[]> list = new ArrayList<byte[]>();
        list.add(Bytes.toBytes("1"));
        list.add(Bytes.toBytes("2"));
        list.add(Bytes.toBytes("3"));
        list.add(Bytes.toBytes("4"));
        list.add(Bytes.toBytes("5"));

        JavaRDD<byte[]> rdd = jsc.parallelize(list);
//        System.out.println(ANSI_CYAN + ">>>>>>>>>>>>>>>>>>>>>>>>>>2" + ANSI_RESET);

        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
//        System.out.println(ANSI_CYAN + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>3" + ANSI_RESET);

        JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
//        System.out.println(ANSI_CYAN + ">>>>>>>>>>>>>>>>>>>>>>>>>>>4" + ANSI_RESET);

       JavaRDD<String> rdd2 =  hbaseContext.bulkGet(tableName, 2, rdd, new GetFunction(), new ResultFunction());
        rdd2.collect().forEach(System.out::println);

        jsc.close();
//        System.out.println(ANSI_CYAN + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>5" + ANSI_RESET);
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
//            System.out.println(ANSI_CYAN + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>6" + ANSI_RESET);
            Iterator<KeyValue> it = result.list().iterator();
            StringBuilder b = new StringBuilder();

//            System.out.println(ANSI_CYAN + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>7" + ANSI_RESET);

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
//            System.out.println(ANSI_CYAN + "b=========================================================== = " + b + ANSI_RESET);
            return b.toString();
        }
    }
}
