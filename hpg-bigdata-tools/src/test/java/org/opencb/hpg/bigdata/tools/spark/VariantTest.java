package org.opencb.hpg.bigdata.tools.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

/**
 * Created by jtarraga on 08/07/16.
 */

// mvn -Dcheckstyle.skip=true -Dtest=VariantTest -DfailIfNoTests=false test

public class VariantTest {

    @Test
    public void execute() {
        SparkConf sparkConf = SparkConfCreator.getConf("MyTest", "local", 1, true);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        VariantDataset vd = new VariantDataset();
        try {
            vd.load("/home/jtarraga/data/spark/episodes.avro", sparkContext);
            vd.filter("doctor > 5").filter("doctor < 11").show();
            vd.filter("doctor > 5").filter("doctor < 11").showMe();
            //vd.select("title").show(); //write().text("/tmp/output/vd.episodes");
            //.write().format("com.databricks.spark.avro").save("/tmp/output");
        } catch (Exception e) {
            e.printStackTrace();
        }

        AlignmentDataset ad = new AlignmentDataset();
        try {
            ad.load("/home/jtarraga/data/spark/episodes.avro", sparkContext);
            ad.filter("doctor > 5").filter("doctor < 11").show();
            ad.filter("doctor > 5").filter("doctor < 11").showMe();
            //vd.select("title").show(); //write().text("/tmp/output/ad.episodes");
            //.write().format("com.databricks.spark.avro").save("/tmp/output");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}