/**
 * 
 */
package org.opencb.hpg.bigdata.core.utils;

import htsjdk.variant.variantcontext.LazyGenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;

import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mh719
 *
 */
public class VariantContextBlockIterator {

	private final VCFCodec codec;
    private boolean decodeGenotypes = true;

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
            if (decodeGenotypes) {
                if (ctx.getGenotypes().isLazyWithData()) {
                    ((LazyGenotypesContext) ctx.getGenotypes()).decode();
                }
            }
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

    public boolean isDecodeGenotypes() {
        return decodeGenotypes;
    }

    public void setDecodeGenotypes(boolean decodeGenotypes) {
        this.decodeGenotypes = decodeGenotypes;
    }


}
