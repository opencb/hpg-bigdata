package org.opencb.hpg.bigdata.core.lib;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * Created by jtarraga on 17/05/17.
 */
public class VariantDatasetFacetTest implements Serializable {
    public static class Employee implements Serializable {
        private String name;
        private long salary;
        private String type;

        public Employee () {
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getSalary() {
            return salary;
        }

        public void setSalary(long salary) {
            this.salary = salary;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }


    @Test
    public void test1() {
        String path;
        SparkConf sparkConf = SparkConfCreator.getConf("Dataset Variant", "local", 1,
                false, "");
        //sparkConf.set();
        //sparkConf.registerKryoClasses(Array(classOf[Variant]));
        //sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        //sparkConf.set("spark.kryoserializer.buffer.max", "512m");
        //sparkConf.set("spark.kryoserializer.buffer", "256m");

        SparkSession spark = new SparkSession(new SparkContext(sparkConf));
        SQLContext sqlContext = new SQLContext(spark);
//        SparkSession spark = SparkSession
//                .builder()
//                .appName("Java Spark SQL user-defined Datasets aggregation example")
//                .getOrCreate();

        // $example on:typed_custom_aggregation$
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        path = "/tmp/employees.json"; //examples/src/main/resources/employees.json";
        Dataset<Employee> ds = spark.read().json(path).as(employeeEncoder);
        ds.show();
        // +-------+------+
        // |   name|salary|
        // +-------+------+
        // |Michael|  3000|
        // |   Andy|  4500|
        // | Justin|  3500|
        // |  Berta|  4000|
        // +-------+------+


        Encoder<Variant> vEncoder = Encoders.bean(Variant.class);
        Variant variant;

/*
        path = "/tmp/kk/1K.vcf.annot.avro.json";
        //String avroPath = "/tmp/employees.json"; //examples/src/main/resources/employees.json";
        //Dataset<Variant> dsVariant = spark.read().json(path).as(vEncoder);
        Dataset<Row> dsVariant = spark.read().json(path);
        dsVariant.show(4);
*/
        path = "/tmp/kk/1K.vcf.annot.avro";
        Dataset<Row> dsRow = sqlContext.read().format("com.databricks.spark.avro").load(path);
        dsRow.show(4);
/*
        JavaRDD<Employee> employees = ds.javaRDD();
        JavaPairRDD<String, Integer> ones = employees.mapToPair(e -> new Tuple2<>(e.getType(), 1));
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((a, b) -> a + b);

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
*/
        spark.stop();
    }
}
