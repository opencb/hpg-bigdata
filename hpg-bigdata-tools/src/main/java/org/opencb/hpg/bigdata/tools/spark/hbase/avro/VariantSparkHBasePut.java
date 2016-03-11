package org.opencb.hpg.bigdata.tools.spark.hbase.avro;

import com.cloudera.spark.hbase.JavaHBaseContext;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;

/**
 * Created by pawan on 10/03/16.
 */
public class VariantSparkHBasePut {

    private static String columnFamily = null;

    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_CYAN = "\u001B[36m";

    public static void main(String[] args) {

        System.out.println(ANSI_CYAN + " length :: " + args.length + ANSI_RESET);
        System.out.println(ANSI_CYAN + " args0 :: " + args[0] + ANSI_RESET);
        System.out.println(ANSI_CYAN + " args1 :: " + args[1] + ANSI_RESET);
        System.out.println(ANSI_CYAN + " args2 :: " + args[2] + ANSI_RESET);
        System.out.println(ANSI_CYAN + " args3 :: " + args[3] + ANSI_RESET);
        if (args.length == 0) {
            System.out
                    .println("JavaHBaseBulkPutExample :  {master}, {tableName}, {column family}, {input file}");
        }

        String master = args[0];
        String tableName = args[1];
        columnFamily = args[2];
        String inputFile = args[3];

        JavaSparkContext sc = new JavaSparkContext(master, "JavaHBaseBulkPutExample");
        sc.addJar("spark.jar");

        Configuration conf = new Configuration();

        JavaRDD<Variant> variantJavaRDD = sc.newAPIHadoopFile(inputFile, AvroKeyInputFormat.class,
                AvroKey.class, NullWritable.class, conf).keys()
                .map((Function<AvroKey, Variant>) avroKey -> new Variant((VariantAvro)avroKey.datum()));

        conf = HBaseConfiguration.create();
        conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

        JavaHBaseContext hbaseContext = new JavaHBaseContext(sc, conf);
        hbaseContext.bulkPut(variantJavaRDD, tableName, new PutFunction(), true);

        sc.stop();
    }

    public static class PutFunction implements Function<Variant, Put> {

        private static final long serialVersionUID = 1L;
        public Put call(Variant v) throws Exception {

            Put put = new Put(Bytes.toBytes(v.getChromosome() + "_" + v.getReference() + "_" + v.getAlternate()));
            put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("v"),
                    Bytes.toBytes(v.getImpl().toString()));
            return put;
        }
    }
}
