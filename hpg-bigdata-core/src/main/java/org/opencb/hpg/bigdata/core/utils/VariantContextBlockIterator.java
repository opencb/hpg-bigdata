/**
 * 
 */
package org.opencb.hpg.bigdata.core.utils;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;

import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mh719
 *
 */
public class VariantContextBlockIterator {

	private final VCFCodec codec;

	/**
	 * 
	 * @param codec
	 */
	public VariantContextBlockIterator(VCFCodec codec) {
		this.codec = codec;
	}
	
	public List<VariantContext> convert(List<CharBuffer> block){
		int size = block.size();
		List<VariantContext> varList = new ArrayList<VariantContext>(size);
		for(CharBuffer buff : block){
			VariantContext ctx = convert(buff);
			varList.add(ctx);
		}		
		return varList ;
	}

	private VariantContext convert(CharBuffer buff) {
		VariantContext ctx = getCodec().decode(buff.toString());
		return ctx;
	}

	public VCFCodec getCodec() {
		return codec;
	}
}
