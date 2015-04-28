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

/**
 * 
 */
package org.opencb.hpg.bigdata.core.converters.variation;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.ga4gh.models.CallSet;
import org.opencb.hpg.bigdata.core.converters.Converter;

/**
 * Create full CallSet for each sample
 * 
 * @author mh719
 *
 */
public class Genotype2CallSet implements Converter<String, CallSet> {
	
	public Genotype2CallSet() {
		// nothing
	}

	@Override
	public CallSet forward(String name) {
		CallSet cs = new CallSet();
		cs.setId(name); // TODO - maybe generate
		cs.setName(name);
		Map<CharSequence, List<CharSequence>> emptyMap = Collections.emptyMap();
		cs.setInfo(emptyMap);
		Long currtime = System.currentTimeMillis();
		cs.setCreated(currtime);
		cs.setUpdated(currtime);
		List<CharSequence> varSet = new LinkedList<CharSequence>();
		cs.setVariantSetIds(varSet );
		return cs;
	}

	@Override
	public String backward(CallSet callSet) {
		return callSet.getId().toString();
	}

}
