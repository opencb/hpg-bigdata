/**
 * 
 */
package org.opencb.hpg.bigdata.core.converters;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.ga4gh.models.Variant;

import com.google.common.collect.Lists;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;

/**
 * @author mh719
 *
 */
public class VariantContext2VariantConverter implements Converter<VariantContext, Variant> {

	public static void main(String[] args) {
		
		Converter<VariantContext,Variant> converter = new VariantContext2VariantConverter();
		File file = new File(args[0]);
		try(VCFFileReader reader = new VCFFileReader(file);){
			VCFHeader header = reader.getFileHeader();
			
			for(VariantContext c : reader){
				Variant v = converter.forward(c);
				print(v);
			}
		}
	}
	
	public static void print (Variant v){
		System.out.println(String.format("%s:%s-%s %s %s %s", 
				v.getReferenceName(),v.getStart(),v.getEnd(),v.getId(),
				v.getReferenceBases(),v.getAlternateBases()));
	}
	
	/*
	 * 
	 
	 VariantSetMetadata represents VCF header information.
	
	`Variant` and `CallSet` both belong to a `VariantSet`.
	`VariantSet` belongs to a `Dataset`. 
	The variant set is equivalent to a VCF file.

	A `CallSet` is a collection of variant calls for a particular sample. It belongs to a `VariantSet`. This is equivalent to one column in VCF.

	A `Call` represents the determination of genotype with respect to a	particular `Variant`.

	An `AlleleCall` represents the determination of the copy number of a particular	`Allele`, possibly within a certain `Variant`.

	A `Variant` represents a change in DNA sequence relative to some reference. For example, a variant could represent a SNP or an insertion.
	Variants belong to a `VariantSet`. This is equivalent to a row in VCF.

	*
	*/
	
	@Override
	public Variant forward(VariantContext c) {
		
		
		
		
		Variant v = new Variant();
		
		/* VCF spec (1-Based): 
		 *  -> POS - position: The reference position, with the 1st base having position 1.
		 *  -> TODO: Telomeres are indicated by using positions 0 or N+1
		 * GA4GH spec 
		 *  -> (0-Based)
		 */
		v.setStart(translateStartPosition(c));
		v.setEnd(Long.valueOf(c.getEnd()));
		v.setReferenceName(c.getChr());
		v.setId(c.getID());		
		
		// Reference
		Allele refAllele = c.getReference();
		v.setReferenceBases(refAllele.getBaseString());
		
		// Alt
		{
			List<Allele> altAllele = c.getAlternateAlleles();
			CharSequence[] altBases = new CharSequence[altAllele.size()];
			int iAlt = 0;
			for(Allele a : altAllele ){
				altBases[iAlt++] = a.getBaseString();
			}
			v.setAlternateBases(Arrays.asList(altBases));
		}
		
		// QUAL
		/** 
		 * TODO: QUAL - quality: Phred-scaled quality score 
		 */
		
		//  FILTER
		/**
		 * TODO: FILTER - filter status
		 */
		
		// INFO
		
		
		// TODO Auto-generated method stub
		return v;
	}

	/**
	 * Translate from 1-based to 0-based
	 * @param c
	 * @return 0-based start position
	 */
	private Long translateStartPosition(VariantContext c) {
		int val = c.getStart();
		if(val == 0)
			return 0l; // TODO: test if this is correct for Telomeres
		return Long.valueOf(val-1);
	}

	@Override
	public VariantContext backward(Variant obj) {
		// TODO Auto-generated method stub
		return null;
	}

}
