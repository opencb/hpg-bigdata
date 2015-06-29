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

import java.util.concurrent.ConcurrentHashMap;

import org.ga4gh.models.CallSet;

/**
 * @author mh719
 *
 */
public class VariantConverterContext {
	
//	public final AtomicReferenceArray<CallSet> callsetArray;
	public final ConcurrentHashMap<CharSequence, CallSet> callSetMap = new ConcurrentHashMap<>();
	
	public VariantConverterContext() {
		
	}

	public ConcurrentHashMap<CharSequence, CallSet> getCallSetMap() {
		return callSetMap;
	}
	
}
