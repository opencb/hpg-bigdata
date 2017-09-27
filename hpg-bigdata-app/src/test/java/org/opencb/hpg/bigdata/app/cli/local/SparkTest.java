package org.opencb.hpg.bigdata.app.cli.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.hpg.bigdata.core.lib.SparkConfCreator;
import org.opencb.hpg.bigdata.core.lib.VariantDataset;
import scala.Tuple2;
import scala.collection.*;
import scala.collection.mutable.WrappedArray;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.Iterator;
import java.util.Map;

import static org.apache.parquet.example.Paper.schema;

public class SparkTest implements Serializable {

    //public String avroDirname = "/home/jtarraga/data150/partitions";
    //public String avroDirname = "/tmp/chr22.head1k.vcf.avro";
    public String avroDirname = "/tmp/example.vcf.avro";
    public VariantDataset vd;
    public SparkSession sparkSession;
    public transient JavaSparkContext sc;

    public Dataset<Row> ds;
    public JavaRDD<Integer> rdd;

    String inParquet = "/tmp/test.vcf.parquet";
    String inParquet2 = "/tmp/test.vcf.parquet2";

//    public class AvgCount implements Serializable {
//        public AvgCount() {
//            total_ = 0;
//            num_ = 0;
//        }
//        public AvgCount(Integer total, Integer num) {
//            total_ = total;
//            num_ = num;
//        }
//        public AvgCount merge(Iterable<Integer> input) {
//            for (Integer elem : input) {
//                num_ += 1;
//                total_ += elem;
//            }
//            return this;
//        }
//        public Integer total_;
//        public Integer num_;
//        public float avg() {
//            return total_ / (float) num_;
//        }
//    }

//    @Test
//    public void test() {
//        //System.out.println("dataset.count = " + ds.count());
//        //ds.show(4);
//
//        FlatMapFunction<Iterator<Row>, AvgCount> mapF = new FlatMapFunction<Iterator<Row>, AvgCount>() {
//            @Override
//            public Iterator<AvgCount> call(Iterator<Row> input) {
//                AvgCount a = new AvgCount(0, 0);
//                while (input.hasNext()) {
//                    a.total_ += 1; //input.next();
//                    a.num_ += 1;
//                }
//                ArrayList<AvgCount> ret = new ArrayList<AvgCount>();
//                ret.add(a);
//                return ret.iterator();
//            }
//        };
//
//        FlatMapFunction<Iterator<Integer>, AvgCount> mapF2 = new FlatMapFunction<Iterator<Integer>, AvgCount>() {
//            @Override
//            public Iterator<AvgCount> call(Iterator<Integer> input) {
//                AvgCount a = new AvgCount(0, 0);
//                while (input.hasNext()) {
//                    a.total_ += input.next();
//                    a.num_ += 1;
//                }
//                ArrayList<AvgCount> ret = new ArrayList<AvgCount>();
//                ret.add(a);
//                return ret.iterator();
//            }
//        };
//
//        Function2<AvgCount, AvgCount, AvgCount> reduceF = new Function2<AvgCount, AvgCount, AvgCount>() {
//            @Override
//            public AvgCount call(AvgCount a, AvgCount b) {
//                a.total_ += b.total_;
//                a.num_ += b.num_;
//                return a;
//            }
//        };
//
//        Iterator<AvgCount> it = ds.toJavaRDD().mapPartitions(mapF).toLocalIterator();
//        while (it.hasNext()) {
//            System.out.println(it.next().avg());
//        }
//        //AvgCount result = rdd.mapPartitions(mapF2).reduce(reduceF);
//        //AvgCount result = ds.toJavaRDD().mapPartitions(mapF).reduce(reduceF);
//        //System.out.println("\n--------------------\n" + result.avg() + "\n--------------------\n");
//    }

    @Test
    public void test2() {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        List<Integer> list = rdd.mapPartitions((Iterator<Integer> iter) -> {
            List<Integer> outIter = new ArrayList<>(1000);
            while (iter.hasNext()) {
                int val = iter.next();
                outIter.add(val + 1000);
            }
            return outIter.iterator();
        }).collect();
        for (int item: list) {
            System.out.println(item);
        }
    }

    @Test
    public void test3() {
        List<String> list = ds.toJSON().toJavaRDD().mapPartitions((Iterator<String> iter) -> {
            ObjectMapper objMapper = new ObjectMapper();
            ObjectReader objectReader = objMapper.readerFor(Variant.class);

            List<String> outIter = new ArrayList<>(1000);
            while (iter.hasNext()) {
                String str = iter.next();
                Variant variant = objectReader.readValue(str);
                outIter.add(variant.getType().name());
            }
            return outIter.iterator();
        }).collect();
        for (String item: list) {
            System.out.println(item);
        }
    }

    @Test
    public void test4() {
        List<Variant> list = ds.toJSON().toJavaRDD().mapPartitions((Iterator<String> iter) -> {
            ObjectMapper objMapper = new ObjectMapper();
            ObjectReader objectReader = objMapper.readerFor(Variant.class);

            List<Variant> outIter = new ArrayList<>(1000);
            while (iter.hasNext()) {
                String str = iter.next();
                Variant variant = objectReader.readValue(str);
                variant.setChromosome("pepe" + variant.getChromosome());
                outIter.add(variant);
            }
            return outIter.iterator();
        }).collect();
        for (Variant item: list) {
            System.out.println(item.toJson());
        }
    }

    @Test
    public void test5() {
        Encoder<Row> encoder = Encoders.kryo(Row.class);
        String outDir = "/tmp/test5/partitions";
        ds.mapPartitions(new MapPartitionsFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Iterator<Row> input) throws Exception {
                List<Row> output = new ArrayList<>(1000);
                while (input.hasNext()) {
                    output.add(input.next());
                }
                return output.iterator();
            }
        }, encoder).write().format("com.databricks.spark.avro").save(outDir);
        //Dataset<Row> ds1 = sparkSession.read().format("com.databricks.spark.avro").load(outDir);
    }

    @Test
    public void test6() {
        //Encoder<VariantAvro> encoder = Encoders.kryo(VariantAvro.class);
        Encoder<VariantAvro> encoder = Encoders.bean(VariantAvro.class);
        String inDir = "/tmp/test6/partitions";

        Dataset<Row> ds1 = sparkSession.read().parquet(inParquet);
        System.out.println("\n\n\nDataset - printSchema");
        ds1.printSchema();
        System.out.println("\n\n\nRow - schema");
        System.out.println(ds1.head().schema());
        System.out.println("\n\n\nRow - mkString");
        System.out.println(ds1.head().schema().mkString());

        //String outDir = "/tmp/test6/partitions";
        //Dataset<VariantAvro> ds1 = sparkSession.read().parquet(inParquet).as(encoder);
        //ds1.toJSON().
        //Dataset<VariantAvro> ds1 = sparkSession.read().parquet(inParquet).as(encoder);
        //Dataset<Row> ds1 = sparkSession.read().parquet(inParquet);
        //ds1.toJSON()
        //Row row;
        //row.schema();
        //ds1.write().parquet(inParquet2);
        //ds1.write().json();
        //ds1.show(true);
        //System.out.println("ds count = " + ds1.count());
/*

        ds.mapPartitions(new MapPartitionsFunction<Variant, Variant>() {
            @Override
            public Iterator<Variant> call(Iterator<Variant> input) throws Exception {
                ObjectMapper objMapper = new ObjectMapper();
                ObjectReader objectReader = objMapper.readerFor(Variant.class);

                List<Variant> output = new ArrayList<>(1000);
                while (input.hasNext()) {
                    Variant variant = objectReader.readValue(input.next().toString());
                    output.add(variant);
                }
                return output.iterator();
            }
        }, encoder).write().format("com.databricks.spark.avro").save(outDir);
        //Dataset<Row> ds1 = sparkSession.read().format("com.databricks.spark.avro").load(outDir);
       */
    }

    public void displayRow(StructField field, int index, Row row, String indent) {
        if (row.get(index) == null) {
            System.out.println(indent + field.name() + " IS NULL");
            return;
        }

//        DataTypes.

        switch (field.dataType().typeName()) {
            case "string":
            case "integer":
                System.out.println(indent + field.name() + ":" + row.get(index) + " -- " + field);
                break;
            case "array":
                System.out.println(field.dataType().json());
                List<String> list = row.getList(index);
                //WrappedArray array = (WrappedArray) obj;
                //Row row;
                //row.getList(i);
                if (list.isEmpty()) {
                    System.out.println(indent + field.name() + ": [] -- " + field);
                } else {
                    System.out.println(indent + field.name() + ": ARRAY NOT EMPTY -- " + field);
                }
                break;
            default:
                System.out.println(field.dataType().json());
                System.out.println(" ?? " + field.dataType().typeName() + " -- " + field);
                break;
        }
    }

    @Test
    public void test7() throws IOException {
        Dataset<Row> ds1 = sparkSession.read().parquet(inParquet);

        ObjectMapper objMapper = new ObjectMapper();
        ObjectReader objectReader = objMapper.readerFor(VariantAvro.class);

        VariantAvro variantAvro = objectReader.readValue(ds1.toJSON().head());
        Row row = RowFactory.create(variantAvro);
        System.out.println("111\n" + ds1.head().toString());
        System.out.println("222\n" + row.toString());
/*
        Row row = ds1.head();
        System.out.println(row.toString());

        scala.collection.Iterator<StructField> it = row.schema().iterator();
        while (it.hasNext()) {
            StructField field = it.next();
            displayRow(field, row.fieldIndex(field.name()), row, "");
            //System.out.println(field.name() + " : " + field.dataType().typeName());
            //System.out.println(field);
        }
        System.out.println(ds1.toJSON().head());


        //sparkSession.sqlContext().ap applySchema(row, row.schema()).toJSON();
/*
        StructType schema = ds1.head().schema();
        JavaRDD<String> jrdd = ds.toJSON().toJavaRDD().mapPartitions((Iterator<String> iter) -> {
            ObjectMapper objMapper = new ObjectMapper();
            ObjectReader objectReader = objMapper.readerFor(Variant.class);

            List<String> outIter = new ArrayList<>(1000);
            while (iter.hasNext()) {
                String str = iter.next();
                Variant variant = objectReader.readValue(str);
                variant.setChromosome("pepe" + variant.getChromosome());
                outIter.add(variant.toJson());
            }
            return outIter.iterator();
        });

        Dataset<Row> dsRow = sparkSession.read().json(jrdd);
        Dataset<Variant> dsVariant = sparkSession.c

       // for (Variant item: list) {
       //     System.out.println(item.toJson());
       // }
*/
    }

    @Before
    public void setUp() throws Exception {
        VariantRvTestsCLITest rvTestsCLITest = new VariantRvTestsCLITest();
        //rvTestsCLITest.init();

        SparkConf sparkConf = SparkConfCreator.getConf("TEST", "local", 1, true);

        sc = new JavaSparkContext(sparkConf);
        sparkSession = new SparkSession(sc.sc());
        //SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));

       // ds = sparkSession.sqlContext().read().format("com.databricks.spark.avro").load(avroDirname);

        rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        //ds = sparkSession.sql("SELECT * FROM vcf");
/*
        vd = new VariantDataset(sparkSession);
        read();
*/
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

//        Broadcast<ObjectReader> broad = sc.broadcast(objectReader);

        List<Integer> list = vd.toJavaRDD().map((Function<Row, Integer>) s -> {
            //System.out.println(s);
            return 1;
        }).collect();

        System.out.println("list size = " + list.size());
    }



    @Test
    public void mapToPair() {
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
