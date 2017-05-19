package org.opencb.hpg.bigdata.tools.variant.analysis;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.junit.Test;
import org.opencb.hpg.bigdata.core.lib.SparkConfCreator;
import org.opencb.hpg.bigdata.core.lib.VariantDataset;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * Created by jtarraga on 13/01/17.
 */
public class JavaCustomReceiver extends Receiver<Dataset<Row>> {
    private static final Pattern SPACE = Pattern.compile(" ");

    // ============= Receiver code that receives data over a socket ==============

    SQLContext sqlContext;

    public JavaCustomReceiver(SQLContext sqlContext) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.sqlContext = sqlContext;
    }

    @Override
    public void onStart() {
        // Start the thread that receives data over a connection
        new Thread()  {
            @Override public void run() {
                receive();
            }
        }.start();
    }

    @Override
    public void onStop() {
        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself if isStopped() returns false
    }

    /**
     * Create a socket connection and receive data until receiver is stopped
     * */
    private void receive() {
        for (int i = 0; i < 5; i++) {
            Dataset<Row> rows = sqlContext.sql("SELECT chromosome, start");
            System.out.println("=================> Stream, dataset " + i + " with " + rows.count() + " rows");
            store(rows);
        }
        System.exit(0);
    }}
