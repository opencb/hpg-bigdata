package org.opencb.hpg.bigdata.tools.spark.hbase;

import com.cloudera.spark.hbase.JavaHBaseContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.List;

public class JavaHBaseBulkPutExample {
  private static final String ANSI_RESET = "\u001B[0m";
  private static final String ANSI_CYAN = "\u001B[36m";

  public static void main(String[] args) {

    System.out.println(ANSI_CYAN + " length :: " + args.length + ANSI_RESET);
    System.out.println(ANSI_CYAN + " args0 :: " + args[0] + ANSI_RESET);
    System.out.println(ANSI_CYAN + " args1 :: " + args[1] + ANSI_RESET);
    System.out.println(ANSI_CYAN + " args2 :: " + args[2] + ANSI_RESET);
    if (args.length == 0) {
      System.out
              .println("JavaHBaseBulkPutExample :  {master}, {tableName}, {columnFamily}");
    }

    String master = args[0];
    String tableName = args[1];
    String columnFamily = args[2];

    JavaSparkContext jsc = new JavaSparkContext(master, "JavaHBaseBulkPutExample");
    jsc.addJar("spark.jar");

    List<String> list = new ArrayList<>();
    list.add("1," + columnFamily + ",a,11");
    list.add("2," + columnFamily + ",a,22");
    list.add("3," + columnFamily + ",a,33");
    list.add("4," + columnFamily + ",a,44");
    list.add("5," + columnFamily + ",a,55");

    JavaRDD<String> rdd = jsc.parallelize(list);

    Configuration conf = HBaseConfiguration.create();
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
    hbaseContext.bulkPut(rdd, tableName, new PutFunction(), true);

    jsc.stop();
  }

  public static class PutFunction implements Function<String, Put> {

    private static final long serialVersionUID = 1L;

    public Put call(String v) throws Exception {
      String[] cells = v.split(",");
      Put put = new Put(Bytes.toBytes(cells[0]));

      put.add(Bytes.toBytes(cells[1]), Bytes.toBytes(cells[2]),
              Bytes.toBytes(cells[3]));
      return put;
    }

  }
}
