/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.hpg.bigdata.core.lib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

/**
 * Created by imedina on 04/08/16.
 */
public class VariantDatasetTest {

    @Test
    public void execute() {
        System.out.println(">>>> Running VariantDatasetTest 0000...");
//        SparkConf sparkConf = SparkConfCreator.getConf("MyTest", "local", 1, true, "/home/imedina/soft/spark-1.6.2");

        SparkConf sparkConf = SparkConfCreator.getConf("MyTest", "local", 1, true, "/home/joaquin/softs/spark-2.0.0-bin-hadoop2.7/bin");
        System.out.println("sparkConf = " + sparkConf.toDebugString());
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        System.out.println(">>>> opening file...");

        //String filename = "/home/imedina/appl/hpg-bigdata/data/CEU-1409-01_20000.vcf.avro";
        String filename = "/tmp/kk/xxx.avro";
        VariantDataset vd = new VariantDataset();
        try {
            vd.load(filename, sparkContext);

            System.out.println("--------------------------------------");
            vd.select(vd.col("studies.files")).show(2);
            System.out.println("--------------------------------------");
        } catch (Exception e) {
            e.printStackTrace();
        }



        /*
            JavaRDD<Row> rdd = vd.javaRDD();
            System.out.println("--------------------------------------");
            System.out.println("------> initial count: " + rdd.count());
            long finalCount = rdd.filter(new Function<Row, Boolean> () {
                @Override
                public Boolean call(Row row) throws Exception {
                    int index = row.fieldIndex("studies");
                    List<Row> studies = row.getList(index);
                    System.out.println("--> index studies: " + index + ", size: " + studies.size());
                    for(int i = 0; i < studies.size(); i++) {
                        Row study = studies.get(i);
                        index = study.fieldIndex("stats");
                        Map stats = study.getMap(index);
                        System.out.println("----> index stats: " + index + ", size: " + stats.size());
                        java.util.Map<String, Row> statsMap = JavaConversions.asJavaMap(stats);
                        for (String key: statsMap.keySet()) {
                            Row stat = statsMap.get(key);
                            float maf = stat.getFloat(stat.fieldIndex("maf"));
                            if (maf > 0.0f) {
                                System.out.println("------> " + statsMap.get(key) + " ==> " + key + ": maf = " + maf);
                                return true;
                            }
                        }
                    }
                    return false;
                }
            }).count();
            System.out.println("------> final count: " + finalCount);
            System.out.println("--------------------------------------");
*/
  /*
            vd.toJavaRDD().map(new Function<Row, Row>() {
                Boolean first = true;
                @Override
                public Row call(Row row) throws Exception {
                    System.out.println("index studies = " + row.fieldIndex("studies"));
                    System.out.println("index studies = " + row.fieldIndex("studies"));
                    Row rStudies = (Row) row.getList(row.fieldIndex("studies")).get(0);
                    Row rStats =
                    Boolean found = true;
                    if (first) {
                        System.out.println(r.toString());
                        //System.out.println("\n---> row: " + row.mkString());
                        first = false;
                    }
                    //Map m = (Map) row.getList(0).get(0);
                    //System.out.println("\n---> map: " + m.mkString());

                    System.out.println("\n---> keySet = " + map.keySet().mkString());
                    java.util.Map<String, Row> javaMap = JavaConversions.asJavaMap(map);
                    System.out.println("map size = " + map.keySet().size());
                    for (String key: javaMap.keySet()) {
                        Row r = javaMap.get(key);
                        float maf = r.getFloat(r.fieldIndex("maf"));
                        if (maf > 0.0f) {
                            found = true;
                            System.out.println("*** " + javaMap.get(key));
                            System.out.println(key + ": maf = " + maf);
                            break;
                        }
                    }
                    return (found ? row : null);
                }
            }).reduce(new Function2<Row, Row, Row>() {
                @Override
                public Row call(Row r1, Row r2) throws Exception {
                    return null;
                }
            });
 */
            /*
            Row row = vd.select("studies.stats").first();
            row.schema().printTreeString();
            System.out.println("----> " + row.get(0).toString());
            Map map = (Map) row.getList(0).get(0);
            System.out.println("\n---> map: " + map.mkString());
            System.out.println("\n---> keySet = " + map.keySet().mkString());
            java.util.Map<String, Row> javaMap = JavaConversions.asJavaMap(map);
            System.out.println("map size = " + map.keySet().size());
            for (String key: javaMap.keySet()) {
                Row r = javaMap.get(key);
                float maf = r.getFloat(r.fieldIndex("maf"));
                if (maf > 0.0f) {
                    System.out.println("*** " + javaMap.get(key));
                    System.out.println(key + ": maf = " + maf);
                }
            }
            //System.out.println("----> " + row.getAs);
  /*
            Row row = vd.select("studies.files").first();
            row.schema().printTreeString();
            System.out.println("row = " + row.mkString());
            System.out.println("row.0 = " + row.getList(0).get(0));
            System.out.println("row.0.0 = " + ((Seq) row.getList(0).get(0))get(0));

/*
            List list = row.getList(0);
            List wa = ((WrappedArray) list.get(0)).toList();
            System.out.println("0 = " + wa.get(0));
            System.out.println("1 = " + wa.get(1));
            System.out.println("2 = " + wa.get(2));
            //System.out.println("----> " + row.getList(0).get(1));

/*
            for (Row row: rows) {
                System.out.println("----> " + row.mkString());

                System.out.println("------> " + row.get(0));
                System.out.println("------> " + row.get(1));
                System.out.println("------> " + row.get(2));
                Map map = row.getMap(2);
                Iterator it = map.keysIterator();
                while (it.hasNext()) {
                    //String key = (String) it.next();
                    System.out.println("--> " + it.next());

                    //System.out.println("key:" + key + " --> value:" + map.get(key));
                }
            }
*/


//
//            try {
//                vd.load("/home/jtarraga/data/spark/episodes.avro", sparkContext);
//                vd.filter("doctor > 5").filter("doctor < 11").show();
//                vd.filter("doctor > 5").filter("doctor < 11").showMe();
//                //vd.select("title").show(); //write().text("/tmp/output/vd.episodes");
//                //.write().format("com.databricks.spark.avro").save("/tmp/output");
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//
//            AlignmentDataset ad = new AlignmentDataset();
//            try {
//                ad.load("/home/jtarraga/data/spark/episodes.avro", sparkContext);
//                ad.filter("doctor > 5").filter("doctor < 11").show();
//                ad.filter("doctor > 5").filter("doctor < 11").showMe();
//                //vd.select("title").show(); //write().text("/tmp/output/ad.episodes");
//                //.write().format("com.databricks.spark.avro").save("/tmp/output");
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
    }
}
