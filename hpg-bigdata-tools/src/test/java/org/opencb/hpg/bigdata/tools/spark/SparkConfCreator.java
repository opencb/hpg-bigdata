package org.opencb.hpg.bigdata.tools.spark;

import org.apache.spark.SparkConf;

/**
 * Created by jtarraga on 08/07/16.
 */
public class SparkConfCreator {

    public static SparkConf getConf(String appName, String master, int numThreads, boolean useKryo) {
        SparkConf sparkConf = new SparkConf()
        .setAppName(appName)
        .setMaster(master + "[" + numThreads + "]");

        if (useKryo) {
            sparkConf = sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        }
        return sparkConf;
    }
}
