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

package org.opencb.hpg.bigdata.tools.spark.datasource;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opencb.biodata.models.variant.Variant;

import java.io.IOException;


/**
 * Created by imedina on 23/01/16.
 */
public class VariantHdfsSparkDataSource extends SparkDataSource<Variant> {


    public VariantHdfsSparkDataSource() {
    }

    public VariantHdfsSparkDataSource(SparkConf sparkConf, JavaSparkContext sparkContext) {
        super(sparkConf, sparkContext);
    }

    @Override
    public JavaRDD<Variant> createRDD() throws IOException {
//        sparkContext.hadoopFile("")
//        Configuration hadoopConf = new Configuration();
//
//        hadoopConf.set("fs.defaultFS", HADOOP_BASE_URI);
//        hadoopConf.set( "avro.schema.input.key", Schema.create( org.apache.avro.Schema.Type.NULL ).toString() ); //$NON-NLS-1$
//        hadoopConf.set( "avro.schema.input.value", Event.SCHEMA$.toString() ); //$NON-NLS-1$
//        hadoopConf.set( "avro.schema.output.key", Schema.create( org.apache.avro.Schema.Type.NULL ).toString() ); //$NON-NLS-1$
////        hadoopConf.set( "avro.schema.output.value", SeverityEventCount.SCHEMA$.toString() );
//
//        JavaRDD<Variant> variantJavaRDD = sparkContext
//                        .newAPIHadoopFile("/user/imedina/CEU-1409-01_50000.vcf", null, Variant.class, NullWritable.class, hadoopConf)
//                        .map(v1-> v1._1());


        return null;
    }

}
