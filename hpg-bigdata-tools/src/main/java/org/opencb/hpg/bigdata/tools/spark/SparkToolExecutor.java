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

package org.opencb.hpg.bigdata.tools.spark;

import org.apache.spark.SparkConf;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by imedina on 23/01/16.
 */
public abstract class SparkToolExecutor {

    private static Map<String, String> sparkToolsRegister;

    public SparkToolExecutor() {
        sparkToolsRegister = new HashMap<>();
    }

    public abstract void execute();

    protected static SparkConf createSparkConf(String appName, String master, int numThreads, boolean useKryo) {
        SparkConf sparkConf = new SparkConf()
                .setAppName(appName)
                .setMaster(master + "[" + numThreads + "]");

        if (useKryo) {
            sparkConf = sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        }
        return sparkConf;
    }

    public static void registerTool(String id, String classpath) {
        if (sparkToolsRegister != null) {
            if (!sparkToolsRegister.containsKey(id)) {
                sparkToolsRegister.put(id, classpath);
            }
        }
    }

    public String getRegisteredTool(String id) {
        if (sparkToolsRegister != null) {
            return sparkToolsRegister.get(id);
        }
        return null;
    }

}
