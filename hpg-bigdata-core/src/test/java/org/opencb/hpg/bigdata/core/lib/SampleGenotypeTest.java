package org.opencb.hpg.bigdata.core.lib;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by jtarraga on 06/10/16.
 */
public class SampleGenotypeTest {
    static VariantDataset vd;
    static SparkConf sparkConf;
    static SparkSession sparkSession;

    @BeforeClass
    public static void setup() {
        sparkConf = SparkConfCreator.getConf("MyTest", "local", 1, true, "/home/joaquin/softs/spark-2.0.0-bin-hadoop2.7/bin");

//        sparkConf.set("spark.broadcast.compress", "true");
//        sparkConf.set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec");
//
//        sparkConf.set("spark.hadoop.mapred.output.compression.codec", "true");
//        sparkConf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
//        sparkConf.set("spark.hadoop.mapred.output.compression.type", "BLOCK");

//        SparkConf sparkConf = SparkConfCreator.getConf("MyTest", "local", 1, true, "/home/imedina/soft/spark-1.6.2");
        //SparkConf sparkConf = SparkConfCreator.getConf("MyTest", "local", 1, true, "/home/imedina/soft/spark-2.0.0");

        System.out.println("sparkConf = " + sparkConf.toDebugString());
        SparkContext sc = new SparkContext(sparkConf);
        sc.setLogLevel("FATAL");
        sparkSession = new SparkSession(sc);
    }

    @AfterClass
    public static void shutdown() {
        vd.sparkSession.sparkContext().stop();
    }

    public void initDataset() {
        vd = new VariantDataset(sparkSession);
        try {
            String filename = this.getClass().getResource("100.variants.avro").getFile();
//            String filename = "/home/jtarraga/data150/spark/variant-test-file-head-200.vcf.annot.avro";
//            String filename = "/home/jtarraga/data150/spark/variant-test-file.vcf.annot.avro";
            System.out.println(">>>> opening file " + filename);
            vd.load(filename, sparkSession);
            //vd.printSchema();
            vd.createOrReplaceTempView("vcf");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void sampleGenotypes() {
        long count = 0;
        try {
            initDataset();

            // query for genotypes
            System.out.println(">>>>> SAMPLE GENOTYPES --------------------------------------");
            vd.sampleFilter("GT", "0:0|0;3:1|0,1|1")
//                 .annotationFilter("consequenceTypes.geneName", "PARP1")
                    .show();
        } catch (Exception e) {
            e.printStackTrace();
        }

        vd.sparkSession.sparkContext().stop();
    }

}
