package org.opencb.hpg.bigdata.core.lib;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by joaquin on 8/21/16.
 */
public class AlignmentDatasetTest {

    static AlignmentDataset ad;
    static SparkConf sparkConf;
    static SparkSession sparkSession;

    @BeforeClass
    public static void setup() {
        // it doesn't matter what we set to spark's home directory
        sparkConf = SparkConfCreator.getConf("AlignmentDatasetTest", "local", 1, true, "");
        System.out.println("sparkConf = " + sparkConf.toDebugString());
        sparkSession = new SparkSession(new SparkContext(sparkConf));
    }

    @AfterClass
    public static void shutdown() {
        ad.sparkSession.sparkContext().stop();
    }

    public void initDataset() {
        ad = new AlignmentDataset(sparkSession);
        try {
            Path inputPath = Paths.get(getClass().getResource("/test.bam.avro").toURI());
            System.out.println(">>>> opening file " + inputPath);
            ad.load(inputPath.toString()); //, sparkSession);
            ad.printSchema();
            ad.createOrReplaceTempView("bam");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void regionFilter() {
        initDataset();
        System.out.println(">>>> Running regionFilter...");

        long count;
        count = sparkSession.sql("select alignment.position.referenceName, alignment.position.position from bam").count();
        System.out.println("count = " + count);

        System.out.println("-------------------------------------- using SQL query");

        sparkSession.sql("select alignment.position.referenceName, alignment.position.position, fragmentName, fragmentLength, length(alignedSequence), alignedSequence from bam where alignment.position.referenceName = \"1\" AND alignment.position.position >= 31915360 AND (alignment.position.position + length(alignedSequence)) <= 31925679").show();

        System.out.println("-------------------------------------- using regionFilter");

        ad.regionFilter("1:31915360-31925679").show();

        System.out.println("--------------------------------------");
    }

    @Test
    public void mapqFilter() {
        initDataset();
        System.out.println(">>>> Running mappingQualityFilter...");

        long count;

        System.out.println("-------------------------------------- using mappingQualityFilter");

        ad.mappingQualityFilter(">50").show();

        System.out.println("--------------------------------------");
    }

    @Test
    public void tlenFilter() {
        initDataset();
        System.out.println(">>>> Running templateLengthFilter...");

        long count;

        System.out.println("-------------------------------------- using templateLengthFilter");

        ad.templateLengthFilter(">398;<400").show();

        System.out.println("--------------------------------------");
    }

    @Test
    public void alenFilter() {
        initDataset();
        System.out.println(">>>> Running alignmentLengthFilter...");

        long count;

        System.out.println("-------------------------------------- using alignmentLengthFilter");

        ad.alignmentLengthFilter(">50;<50").show();

        System.out.println("--------------------------------------");
    }


    @Test
    public void flagFilter() {
        System.out.println(">>>> Running flagFilter...");

        long count;

        System.out.println("-------------------------------------- using flagFilter");

        initDataset();
        //sparkSession.sql("select * from bam").show();
        ad.flagFilter("147,99").show();

        initDataset();
        ad.flagFilter("83", true).show();

        System.out.println("--------------------------------------");
    }
}
