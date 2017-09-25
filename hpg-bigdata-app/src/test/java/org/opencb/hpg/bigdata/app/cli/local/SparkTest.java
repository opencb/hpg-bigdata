package org.opencb.hpg.bigdata.app.cli.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.hpg.bigdata.core.lib.SparkConfCreator;
import org.opencb.hpg.bigdata.core.lib.VariantDataset;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SparkTest implements Serializable {

    public VariantDataset vd;
    SparkSession sparkSession;
    public JavaSparkContext sc;

    @Before
    public void setUp() throws Exception {
        VariantRvTestsCLITest rvTestsCLITest = new VariantRvTestsCLITest();
        //rvTestsCLITest.init();

        SparkConf sparkConf = SparkConfCreator.getConf("TEST", "local", 1, false);

        sc = new JavaSparkContext(sparkConf);
        sparkSession = new SparkSession(sc.sc());
        //SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));

        vd = new VariantDataset(sparkSession);
        read();
/*
        try {
            String filename = "/tmp/chr22.head1k.vcf.avro";
            //String filename = "/tmp/chr22.head100k.vcf.avro";
            //String filename = rvTestsCLITest.avroPath.toString();

            vd.load(filename);
        } catch (Exception e) {
            e.printStackTrace();
        }
        vd.createOrReplaceTempView("vcf");
*/
    }

    @After
    public void tearDown() {
        sparkSession.stop();
    }

    @Test
    public void map() {
        ObjectMapper objMapper = new ObjectMapper();
        ObjectReader objectReader = objMapper.readerFor(Variant.class);

        Broadcast<ObjectReader> broad = sc.broadcast(objectReader);

        List<Tuple2<String, Integer>> list = vd.toJSON().toJavaRDD().mapToPair((PairFunction<String, String, Integer>) s -> {
            Variant variant = broad.getValue().readValue(s);
            String key = variant.getChromosome();
            int value = variant.getStart();
            return new Tuple2<>(key, value);
        }).collect();
        for (Tuple2<String, Integer> item: list) {
            System.out.println(item._1 + " -> " + item._2);
        }
    }

    @Test
    public void reduce() {
        ObjectMapper objMapper = new ObjectMapper();
        ObjectReader objectReader = objMapper.readerFor(Variant.class);

        Broadcast<ObjectReader> broad = sc.broadcast(objectReader);

        List<Tuple2<String, Integer>> list = vd.toJSON().toJavaRDD().mapToPair((PairFunction<String, String, Integer>) s -> {
            Variant variant = broad.getValue().readValue(s);
            String key = "" + variant.getStart(); //variant.getChromosome();
            int value = variant.getStart();
            return new Tuple2<>(key, value);
        }).reduceByKey((Function2<Integer, Integer, Integer>) (i1, i2) -> {
            return i1 + i2;
        }).collect();
        for (Tuple2<String, Integer> item: list) {
            System.out.println(item._1 + " -> " + item._2);
        }
    }

    @Test
    public void reduceStats() {
        ObjectMapper objMapper = new ObjectMapper();
        ObjectReader objectReader = objMapper.readerFor(Variant.class);

        Broadcast<ObjectReader> broad = sc.broadcast(objectReader);

        List<Tuple2<String, VariantSetStats>> list = vd.toJSON().toJavaRDD().mapToPair((PairFunction<String,
                String, VariantSetStats>) s -> {
            Variant variant = broad.getValue().readValue(s);
            String key = "" + variant.getStart(); //variant.getChromosome();
            VariantSetStats value = new VariantSetStats();
            value.setNumVariants(1);
            return new Tuple2<>(key, value);
        }).reduceByKey((Function2<VariantSetStats, VariantSetStats, VariantSetStats>) (stats1, stats2) -> {
            stats1.setNumVariants(stats1.getNumVariants() + stats2.getNumVariants());
            return stats1;
        }).collect();
        for (Tuple2<String, VariantSetStats> item: list) {
            System.out.println(item._1 + " -> " + item._2().getNumVariants());
        }
    }

    @Test
    public void write() {
        String tmpDir = "/home/jtarraga/data150/partitions";
        vd.repartition(10).write().format("com.databricks.spark.avro").save(tmpDir);
        //vd.coalesce(10).write().format("com.databricks.spark.avro").save(tmpDir);
        //vd.write().format("com.databricks.spark.avro").save(tmpDir);
    }

    @Test
    public void read() {
        String tmpDir = "/home/jtarraga/data150/partitions";
        try {
            vd.load(tmpDir);
            vd.createOrReplaceTempView("vcf");
            partition();
            System.out.println("number of rows = " + vd.count());
        } catch (Exception e) {
            e.printStackTrace();
        }
        //vd.coalesce(10).write().format("com.databricks.spark.avro").save(tmpDir);
        //vd.write().format("com.databricks.spark.avro").save(tmpDir);
    }

    @Test
    public void partition() {
        List<Partition> list = vd.toJSON().toJavaRDD().partitions();
        for (Partition item: list) {
            System.out.println(item.toString());
        }
    }

    @Test
    public void mapPartition() {
        List<Partition> list = vd.toJSON().toJavaRDD().mapPartitions(
                new FlatMapFunction<Iterator<String>, VariantSetStats>() {
            @Override
            public Iterator<VariantSetStats> call(Iterator<String> stringIterator) throws Exception {
                return null;
            }
        }).partitions(); //.mapPartitions()partitions();
        for (Partition item: list) {
            System.out.println(item.toString());
        }
    }


    @Test
    public void reduceStatsMap() {
        ObjectMapper objMapper = new ObjectMapper();
        ObjectReader objectReader = objMapper.readerFor(Variant.class);

        Broadcast<ObjectReader> broad = sc.broadcast(objectReader);

        List<Tuple2<String, Map<String, VariantSetStats>>> list = vd.toJSON().toJavaRDD().mapToPair((PairFunction<String,
                String, Map<String, VariantSetStats>>) s -> {
            Variant variant = broad.getValue().readValue(s);
            String key = "" + variant.getStart(); //variant.getChromosome();
            Map<String, VariantSetStats> value = new HashMap();
            VariantSetStats stats = new VariantSetStats();
            stats.setNumVariants(1);
            value.put("" + variant.getStart(), stats);
            return new Tuple2<>(key, value);
        }).reduceByKey((Function2<Map<String, VariantSetStats>,
                Map<String, VariantSetStats>, Map<String, VariantSetStats>>) (stats1, stats2) -> {
            for (String key: stats2.keySet()) {
                if (stats1.containsKey(key)) {
                    stats1.get(key).setNumVariants(stats1.get(key).getNumVariants() + stats2.get(key).getNumVariants());
                } else {
                    stats1.put(key, stats2.get(key));
                }
            }
            return stats1;
        }).collect();
        for (Tuple2<String, Map<String, VariantSetStats>> item: list) {
            System.out.println(item._1 + " -> ");
            Map<String, VariantSetStats> map = item._2();
            for (String key: map.keySet()) {
                System.out.println("\t" + key + ", num. variants = " + map.get(key).getNumVariants());
            }
        }
    }

}
