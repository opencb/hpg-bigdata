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

/**
 * Created by imedina on 04/08/16.
 */
public class SparkConfCreator {

    public static SparkConf getConf(String appName, String master, int numThreads, boolean useKryo) {
        SparkConf sparkConf = new SparkConf()
                .setAppName(appName)
                .setMaster(master + "[" + numThreads + "]");

        if (useKryo) {
            sparkConf = sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        }
        return sparkConf;
    }

    public static SparkConf getConf(String appName, String master, int numThreads, boolean useKryo, String sparkHome) {
        SparkConf sparkConf = new SparkConf()
                .setSparkHome(sparkHome)
                .setAppName(appName)
                .setMaster(master + "[" + numThreads + "]");

        if (useKryo) {
            sparkConf = sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        }
        return sparkConf;
    }
}
