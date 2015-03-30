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
