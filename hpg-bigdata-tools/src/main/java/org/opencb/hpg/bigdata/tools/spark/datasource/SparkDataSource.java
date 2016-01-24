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

import java.io.IOException;

/**
 * Created by imedina on 23/01/16.
 */
public abstract class SparkDataSource<T> {

    protected SparkConf sparkConf;
    protected JavaSparkContext sparkContext;

    public SparkDataSource() {
    }

    public SparkDataSource(SparkConf sparkConf, JavaSparkContext sparkContext) {
        this.sparkConf = sparkConf;
        this.sparkContext = sparkContext;
    }

    public abstract JavaRDD<T> createRDD() throws IOException;


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SparkDataSource{");
        sb.append("sparkConf=").append(sparkConf);
        sb.append(", sparkContext=").append(sparkContext);
        sb.append('}');
        return sb.toString();
    }

    public SparkConf getSparkConf() {
        return sparkConf;
    }

    public void setSparkConf(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
    }

    public JavaSparkContext getSparkContext() {
        return sparkContext;
    }

    public void setSparkContext(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

}
