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

package org.opencb.hpg.bigdata.core.converters.variation;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.ga4gh.models.CallSet;
import org.opencb.biodata.tools.BiConverter;

/**
 * Create full CallSet for each sample.
 *
 * @author mh719
 */
@Deprecated
public class Genotype2CallSet implements BiConverter<String, CallSet> {

    public Genotype2CallSet() {
    }

    @Override
    public CallSet to(String name) {
        CallSet cs = new CallSet();
        cs.setId(name); // TODO - maybe generate
        cs.setName(name);
        Map<String, List<String>> emptyMap = Collections.emptyMap();
        cs.setInfo(emptyMap);
        Long currentTimeMillis = System.currentTimeMillis();
        cs.setCreated(currentTimeMillis);
        cs.setUpdated(currentTimeMillis);
        List<String> varSet = new LinkedList<>();
        cs.setVariantSetIds(varSet);
        return cs;
    }

    @Override
    public String from(CallSet callSet) {
        return callSet.getId().toString();
    }

}
