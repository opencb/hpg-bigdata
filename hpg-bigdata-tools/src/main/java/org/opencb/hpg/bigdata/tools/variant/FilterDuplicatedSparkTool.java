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

package org.opencb.hpg.bigdata.tools.variant;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.hpg.bigdata.tools.spark.SparkToolExecutor;
import org.opencb.hpg.bigdata.tools.spark.datasource.SparkDataSource;
import org.opencb.hpg.bigdata.tools.spark.datasource.VcfSparkDataSource;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by imedina on 23/01/16.
 */
public class FilterDuplicatedSparkTool extends SparkToolExecutor {

    private SparkDataSource<Variant> sparkDataSource;

    public FilterDuplicatedSparkTool(SparkDataSource<Variant> sparkDataSource) {
        registerTool("FilterDuplicated", this.getClass().toString());
        this.sparkDataSource = sparkDataSource;
    }

    @Override
    public void execute() {
        try {
            // A Variant RDD is obtained from a Spark Data Source, this can be VCF or Avro in HDFS for instance
            JavaRDD<Variant> vcfRdd = sparkDataSource.createRDD();
            List<Tuple2<String, Integer>> duplicated = vcfRdd
                    .mapToPair(variant -> new Tuple2<>(variant.getChromosome() + "_" + variant.getStart() + "_" + variant.getReference(), 1))
                    .reduceByKey((a, b) -> a + b)
                    .filter(v -> v._2 > 1).collect();

            for (Tuple2<String, Integer> stringIntegerTuple2 : duplicated) {
                System.out.println(stringIntegerTuple2);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        SparkConf sparkConf = createSparkConf("FilterDuplicated", "local", 2, true);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // Some checks are needed here
        Path path = Paths.get(args[0]);

        // In the future args need to be evaluated to decide which particular instance of SparkDataSource is created: Vcf, Avro, ...
        VcfSparkDataSource sparkDataSource = new VcfSparkDataSource(sparkConf, sparkContext, path);

        FilterDuplicatedSparkTool filterDuplicatedSparkTool = new FilterDuplicatedSparkTool(sparkDataSource);
        filterDuplicatedSparkTool.execute();
    }

}
