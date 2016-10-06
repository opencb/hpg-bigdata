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
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalog.Column;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.collection.immutable.Seq;

import java.io.File;

/**
 * Created by jtarraga on 04/09/16.
 */
public class GroupByTest {

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
        vd = new VariantDataset();
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
    public void groupby() {
        long count = 0, count1 = 0, count2 = 0, count3 = 0;
        try {
            initDataset();
            System.out.println(">>>>> GROUPBY --------------------------------------");
            //vd
            //.studyFilter("stats.refAlleleCount", "hgva@hsapiens_grch37:1000GENOMES_phase_3::ASW==121")
                    //.annotationFilter("populationFrequencies.altAlleleFreq",
//                            "1000GENOMES_phase_3::ALL<0.01,EXAC::ALL<0.01")
                    //.annotationFilter("consequenceTypes.sequenceOntologyTerms.name", "missense_variant")
                    //.annotationFilter("consequenceTypes.proteinVariantAnnotation.substitutionScores", "sift<0.2")
//                    .annotationFilter("consequenceTypes.geneName", "CCDC66,ITIH1,PCBP4")
                    //.annotationFilter("conservation", "phylop<0.3,phastCons<0.1")
            //.annotationFilter("consequenceTypes.geneName", "O")
//                    .groupBy("geneName").count().show();
              //      .groupBy("ct.geneName").count()
//                    .show();
            ;

            StringBuilder sql;


            // group by gene name
            sql = new StringBuilder();
            sql.append("SELECT geneName as gene_name, count(geneName) as counter FROM (");
            sql.append("SELECT chromosome, start, end, alternate, ct.geneName FROM vcf ");
            sql.append("LATERAL VIEW explode(annotation.consequenceTypes) act as ct ");
            sql.append("GROUP BY chromosome, start, end, alternate, ct.geneName ");
            sql.append(") t2 GROUP BY geneName");
            vd.executeSql(sql.toString()).show();

//            sql = new StringBuilder();
//            sql.append("SELECT chromosome, start, end, ct.geneName FROM vcf ");
//            sql.append("LATERAL VIEW explode(annotation.consequenceTypes) act as ct ");
//            vd.executeSql(sql.toString()).dropDuplicates("chromosome", "start", "end").groupBy("geneName").count().show();

            // group by consequence type
//            sql = new StringBuilder();
//            sql.append("SELECT dct as consequence_type, count(dct) as counter FROM (");
//            sql.append("SELECT chromosome, start, end, alternate, annotation.displayConsequenceType as dct FROM vcf GROUP BY chromosome, start, end, alternate, annotation.displayConsequenceType");
//            sql.append(") t2 GROUP BY dct");
//            vd.executeSql(sql.toString()).show();
////            sql.append("SELECT chromosome, start, end, alternate, annotation.displayConsequenceType FROM vcf");
////            vd.executeSql(sql.toString()).dropDuplicates("chromosome", "start", "end", "alternate").groupBy("displayConsequenceType").count().show();



            // query for genotypes
            sql = new StringBuilder();
            sql.append("SELECT id, chromosome, start, end, alternate, s.samplesData[0][0], s.samplesData[3][0], s.samplesData  ");
            sql.append("FROM vcf ");
            sql.append("LATERAL VIEW explode(studies) act as s ");
            sql.append("LATERAL VIEW explode(s.samplesData) act as ssd ");
//            //sql.append("LATERAL VIEW explode(ssd) act as ssda ");
            sql.append("WHERE ");
////            sql.append("(id = 'esv2659399' OR id = 'rs1137005') ");
////            sql.append("AND ");
            sql.append("(s.samplesData[0][0] = '0|0' AND s.samplesData[3][0] = '1|0')");
//            //sql.append("(ct.geneName = 'CCDC66' OR ct.geneName = 'ITIH1' OR ct.geneName = 'PCBP4')");
//
//
////            vd.executeSql(sql.toString()).showString(20, false));
            System.out.println(vd.executeSql(sql.toString()).dropDuplicates("chromosome", "start", "end", "alternate").showString(300, false));

//            vd.update();
//            vd.coalesce(1).write().format("json").save("/tmp/query.out.json");

            System.out.println(">>>>> count = " + count + " ----------------------------------------------------");
        } catch (Exception e) {
            e.printStackTrace();
        }

        vd.sparkSession.sparkContext().stop();
    }

}
