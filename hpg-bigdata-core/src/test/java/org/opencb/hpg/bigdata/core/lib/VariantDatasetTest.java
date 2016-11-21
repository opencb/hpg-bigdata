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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by imedina on 04/08/16.
 */
public class VariantDatasetTest {

    static VariantDataset vd;
    static SparkConf sparkConf;
    static SparkSession sparkSession;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @BeforeClass
    public static void setup() {
        // it doesn't matter what we set to spark's home directory
        sparkConf = SparkConfCreator.getConf("AlignmentDatasetTest", "local", 1, true, "");
        System.out.println("sparkConf = " + sparkConf.toDebugString());
    }

    @AfterClass
    public static void shutdown() {
//        vd.sparkSession.sparkContext().stop();
    }

    public void initDataset() {
        sparkSession = new SparkSession(new SparkContext(sparkConf));
        vd = new VariantDataset(sparkSession);
        try {
            String filename = this.getClass().getResource("/100.variants.avro").getFile();
            System.out.println(">>>> opening file " + filename);
            vd.load(filename, sparkSession);
            vd.printSchema();
            vd.createOrReplaceTempView("vcf");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void outputFormats() {
        long count = 0;
        try {
            initDataset();
            System.out.println("--------------------------------------");

            //vd.studyFilter("stats.maf", "1000g::all<=0.4").show();
            //vd.studyFilter("stats.maf", "hgva@hsapiens_grch37:1000GENOMES_phase_3::ASW==0.008196721").show();
            vd.studyFilter("stats.refAlleleCount", "hgva@hsapiens_grch37:1000GENOMES_phase_3::ASW==121");

            vd.update();

            // save the dataset according to the output format
//            String format = "avro";
//            String format = "parquet";
            String format = "json";

            String filename = "/tmp/query.out." + format;
            String tmpDir = filename + ".tmp";

            if ("json".equals(format)) {
                vd.coalesce(1).write().format("json").save(tmpDir);
            } else if ("parquet".equals(format)) {
                vd.coalesce(1).write().format("parquet").save(tmpDir);
            } else {
                vd.coalesce(1).write().format("com.databricks.spark.avro").save(tmpDir);
                format = "avro";
            }

            File dir = new File(tmpDir);
            if (!dir.isDirectory()) {
                // error management
                System.err.println("Error: a directory was expected but " + tmpDir);
                return;
            }

            // list out all the file name and filter by the extension
            Boolean found = false;
            String[] list = dir.list();
            for (String name: list) {
                if (name.startsWith("part-r-") && name.endsWith(format)) {
                    new File(tmpDir + "/" + name).renameTo(new File(filename));
                    found = true;
                    break;
                }
            }
            if (!found) {
                // error management
                System.out.println("Error: pattern 'part-r-*" + format + "' was not found");
            }

            // delete temporary directory
            FileUtils.deleteDirectory(dir);
        } catch (Exception e) {
            e.printStackTrace();
        }

        vd.sparkSession.sparkContext().stop();
    }

    @Test
    public void filters() {

        long count = 0;
        try {
            initDataset();
            System.out.println("--------------------------------------");

            //vd.studyFilter("stats.maf", "1000g::all<=0.4").show();
            //vd.studyFilter("stats.maf", "hgva@hsapiens_grch37:1000GENOMES_phase_3::ASW==0.008196721").show();
//            vd.studyFilter("stats.refAlleleCount", "hgva@hsapiens_grch37:1000GENOMES_phase_3::ASW==121").show();

//            long count = vd.annotationfilter("consequenceTypes.sequenceOntologyTerms.accession", "SO:0001566").count();

//            count = vd.annotationFilter("populationFrequencies.altAlleleFreq", "1000G:CEU < 1.2,1000G:ASW < 1.25")
//                    .count();
//            vd.annotationfilter("populationFrequencies", "1000G:ASW < 0.2").count();

//            long count = vd.annotationfilter("consequenceTypes.sequenceOntologyTerms.accession", "SO:0001566")
//            long count = vd//.annotationfilter("consequenceTypes.sequenceOntologyTerms.name", "missense_variant")
//                    .annotationFilter("conservation.phylop", "< 0.2")
//                    .annotationFilter("conservation.phastCons", "< 0.4")
//                    .idFilter("rs587604674")
//                    .count();


//            String ids = "rs587604674,rs587603352";
//            count = vd.idFilter(Arrays.asList(StringUtils.split(ids, ","))).count();

//            count = vd.annotationFilter("conservation", "phylop<0.3,phastCons<0.1").count();
            String types = "SNP,SNV";
            count = vd.typeFilter(new ArrayList<>(Arrays.asList(StringUtils.split(types, ",")))).count();

//            System.out.println(vd.annotationfilter("consequenceTypes.sequenceOntologyTerms.name", "missense_variant")
//                    .select("annotation.consequenceTypes.sequenceOntologyTerms").count());

//            System.out.println(vd.idfilter("rs587604674").count());
//            System.out.println(vd.annotationfilter("id", "ENSG00000233866").count());
            //scala> spark.sql("select * from v10k lateral view explode(annotation.consequenceTypes) act as ct lateral view explode(ct.sequenceOntologyTerms) ctso as so where so.accession = 'SO:0001566'").count()
            //res6: Long = 4437

//            vd.select(vd.col("studies")).show(2);
            System.out.println("--------------------------------------");
            System.out.println("count = " + count);
            System.out.println("--------------------------------------");
//            System.out.println(vd.count());
//            System.out.println(vd.filter("start >= 564477").filter("end <= 729948").count());
//            System.out.println(vd.groupBy("chromosome").avg("start").sort("chromosome").take(3)[0]);
//            System.out.println(vd.describe("studies"));
//            System.out.println("---->>> " + vd.select("studies.files").head());
//            System.out.println("---->>> " + vd.select("studies.files").select("attributes").head());
//            System.out.println(vd.filter("studies.files[0].attributes.AF = '0.009'"));
//            System.out.println(vd.filter("studies.files.attributes.AF = '0.009'"));
            vd.sparkSession.sparkContext().stop();

            assert(count == 98);
        } catch (Exception e) {
            e.printStackTrace();
        }
        vd.sparkSession.sparkContext().stop();
    }

    @Test
    public void groupby() {

        long count = 0;
        try {
            initDataset();
            System.out.println(">>>>> GROUPBY --------------------------------------");
            vd
            //.studyFilter("stats.refAlleleCount", "hgva@hsapiens_grch37:1000GENOMES_phase_3::ASW==121")
                  //  .annotationFilter("consequenceTypes.proteinVariantAnnotation.substitutionScores", "sift< 0.2")
                    //.annotationFilter("conservation", "phylop<0.3,phastCons<0.1")
                    .countBy("gene").show();
              //      .groupBy("ct.geneName").count()
              //      .show();
            System.out.println("----------------------------------------------------");
        } catch (Exception e) {
            e.printStackTrace();
        }

        vd.sparkSession.sparkContext().stop();
    }

}
