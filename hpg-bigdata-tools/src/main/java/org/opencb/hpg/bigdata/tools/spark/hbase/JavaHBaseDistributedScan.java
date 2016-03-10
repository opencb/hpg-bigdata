package org.opencb.hpg.bigdata.tools.spark.hbase;


import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.cloudera.spark.hbase.JavaHBaseContext;

import scala.Tuple2;
import scala.Tuple3;

public class JavaHBaseDistributedScan {
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_CYAN = "\u001B[36m";
    private static final String ANSI_YELLOW = "\u001B[33m";

  public static void main(String[] args) {
    if (args.length == 0) {
      System.out
          .println("JavaHBaseDistributedScan  {master} {tableName}");
    }

    String master = args[0];
    String tableName = args[1];

    JavaSparkContext jsc = new JavaSparkContext(master,
        "JavaHBaseDistributedScan");
    jsc.addJar("spark.jar");


    Configuration conf = HBaseConfiguration.create();
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

    Scan scan = new Scan();
    scan.setCaching(100);
    JavaRDD<Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>>> javaRdd = hbaseContext.hbaseRDD(tableName, scan);
    List<Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>>> results = javaRdd.collect();
    results.size();
    System.out.println(ANSI_CYAN + "results size = " + results.size() + ANSI_RESET);

    ListIterator<Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>>> it1 = results.listIterator();
//      Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>> it1 = results.get(0);
//      byte[] val = new byte[0];
//      byte[] val1 = new byte[0];
//      byte[] val2 = new byte[0];
//      byte[] val3 = new byte[0];

    while (it1.hasNext()) {

//        System.out.println(ANSI_CYAN +"itr1 = " + it1.nextIndex() + ANSI_RESET);
//        val = it1.next()._1();
//        val1 = it1.next()._2().get(0)._1();
//        val2 = it1.next()._2().get(0)._2();
//        val3 = it1.next()._2().get(0)._3();

//            System.out.println(ANSI_CYAN +"rowkey = " + Bytes.toString(it1.next()._1()) + ANSI_RESET);
            List<Tuple3<byte[], byte[], byte[]>> s = it1.next()._2();
            for (int i = 0; i < s.size(); i++) {
                Tuple3<byte[], byte[], byte[]> s1 = s.get(i);
                System.out.println("family : " + Bytes.toString(s1._1()) + " column : " + Bytes.toString(s1._2())
                        + "  value : " + Bytes.toString(s1._3()));
            }
    //        System.out.println(ANSI_CYAN +"rowkey = " + Bytes.toString(it1.next()._1()) + ANSI_RESET);
    //        System.out.println(ANSI_CYAN +"colfamily = " + Bytes.toString(it1.next()._2().get(0)._1())  + ANSI_RESET);
    //        System.out.println(ANSI_CYAN +"col = " + Bytes.toString(it1.next()._2().get(0)._2())  + ANSI_RESET);
    //        System.out.println(ANSI_CYAN +"val = " + Bytes.toString(it1.next()._2().get(0)._3())  + ANSI_RESET);
    }
  }
}
