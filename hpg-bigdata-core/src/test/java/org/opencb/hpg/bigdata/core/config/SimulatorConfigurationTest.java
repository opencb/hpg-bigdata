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

package org.opencb.hpg.bigdata.core.config;

import org.junit.Test;
import org.opencb.biodata.models.core.Region;

import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 08/10/15.
 */
public class SimulatorConfigurationTest {

    @Test
    public void testSerialize() throws Exception {

        SimulatorConfiguration simulatorConfiguration = new SimulatorConfiguration();

        List<Region> regions = Arrays.asList(Region.parseRegion("1"), Region.parseRegion("2:40000-50000"), Region.parseRegion("3:1000000"));

        Map<String, Float> genProb = new HashMap<>();
        genProb.put("0/0", 0.7f);
        genProb.put("0/1", 0.2f);
        genProb.put("1/1", 0.08f);
        genProb.put("./.", 0.02f);

        simulatorConfiguration.setRegions(regions);
        simulatorConfiguration.setGenotypeProbabilities(genProb);

        simulatorConfiguration.serialize(new FileOutputStream("/tmp/hpg-bigdata-simulator-configuration-test.yml"));
    }
}