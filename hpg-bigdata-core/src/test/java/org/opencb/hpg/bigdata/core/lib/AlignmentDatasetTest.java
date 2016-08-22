package org.opencb.hpg.bigdata.core.lib;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

/**
 * Created by joaquin on 8/21/16.
 */
public class AlignmentDatasetTest {

    public void execute() {
        System.out.println(">>>> Running AlignmentDatasetTest 0000...");
//        SparkConf sparkConf = SparkConfCreator.getConf("MyTest", "local", 1, true, "/home/imedina/soft/spark-1.6.2");
        //SparkConf sparkConf = SparkConfCreator.getConf("MyTest", "local", 1, true, "/home/imedina/soft/spark-2.0.0");

        SparkConf sparkConf = SparkConfCreator.getConf("MyTest", "local", 1, true, "/home/joaquin/softs/spark-2.0.0-bin-hadoop2.7/bin");
        System.out.println("sparkConf = " + sparkConf.toDebugString());
        SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));

        System.out.println(">>>> opening file...");

        String filename = "/home/jtarraga/CAM/data/test.bam.avro";

        long count;
//        String filename = "/tmp/kk/xxx.avro";
        AlignmentDataset ad = new AlignmentDataset();
        try {
            ad.load(filename, sparkSession);
            ad.printSchema();

            ad.createOrReplaceTempView("bam");

            count = sparkSession.sql("select alignment.position.referenceName, alignment.position.position from bam").count();
            System.out.println("count = " + count);
            sparkSession.sql("select alignment.position.referenceName, alignment.position.position, fragmentName, fragmentLength, length(alignedSequence), alignedSequence from bam where alignment.position.referenceName = \"1\" AND alignment.position.position >= 31915360 AND (alignment.position.position + length(alignedSequence)) <= 31925679").show();


            System.out.println("--------------------------------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
