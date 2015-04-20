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
	public final ConcurrentHashMap<CharSequence, CallSet> callSetMap = new ConcurrentHashMap<CharSequence, CallSet>();
	
	public VariantConverterContext() {
		
	}

	public ConcurrentHashMap<CharSequence, CallSet> getCallSetMap() {
		return callSetMap;
	}
	
}
