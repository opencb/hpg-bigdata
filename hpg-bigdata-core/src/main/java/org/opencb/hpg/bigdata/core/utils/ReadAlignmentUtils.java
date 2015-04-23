package org.opencb.hpg.bigdata.core.utils;

import org.ga4gh.models.CigarUnit;
import org.ga4gh.models.LinearAlignment;
import org.ga4gh.models.ReadAlignment;
import org.ga4gh.models.Strand;

public class ReadAlignmentUtils {
	private static final String FIELD_SEPARATOR = "\t";

	public static String getSamString(ReadAlignment ra) {
		String res = new String();

		LinearAlignment la = (LinearAlignment) ra.getAlignment();

		// id
		res = ra.getId().toString();
		res += FIELD_SEPARATOR;

		// flags
		int flags = 0;
		if (ra.getNumberReads() > 0) flags |= 0x1;
		if (ra.getProperPlacement()) flags |= 0x2;
		if (la == null) {
			flags |= 0x4;
		} else {
			if (la.getPosition().getStrand() == Strand.NEG_STRAND) {
				flags |= 0x10;
			}
		}
		if (ra.getNextMatePosition() != null) {
			if (ra.getNextMatePosition().getStrand() == Strand.NEG_STRAND) {
				flags |= 0x20;
			}
		} else {
			if (ra.getNumberReads() > 0) {
				flags |= 0x8;
			}
		}
		if (ra.getReadNumber() == 0) flags |= 0x40;
		if (ra.getNumberReads() > 0 && ra.getReadNumber() == ra.getNumberReads() - 1) flags |= 0x80;
		if (ra.getSecondaryAlignment()) flags |= 0x100;
		if (ra.getFailedVendorQualityChecks()) flags |= 0x200;
		if (ra.getDuplicateFragment()) flags |= 0x400;
		res += ("" + flags);
		res += FIELD_SEPARATOR;

		// chromosome
		res += (la.getPosition().getReferenceName().toString());
		res += FIELD_SEPARATOR;

		// position
		res += ("" + la.getPosition().getPosition());
		res += FIELD_SEPARATOR;

		// mapping quality
		res += ("" + la.getMappingQuality());
		res += FIELD_SEPARATOR;

		// cigar
		for(CigarUnit e: la.getCigar()) {
			res += ("" + e.getOperationLength());
			switch (e.getOperation()) {
			case ALIGNMENT_MATCH:
				res += "M";
				break;
			case INSERT:
				res += "I";
				break;
			case DELETE:
				res += "D";
				break;
			case SKIP:
				res += "N";
				break;
			case CLIP_SOFT:
				res += "S";
				break;
			case CLIP_HARD:
				res += "H";
				break;
			case PAD:
				res += "P";
				break;
			case SEQUENCE_MATCH:
				res += "=";
				break;
			case SEQUENCE_MISMATCH:
				res += "X";
				break;
			}
		}
		res += FIELD_SEPARATOR;

		// mate chromosome 
		if (ra.getNextMatePosition() != null) {
			if (ra.getNextMatePosition().getReferenceName().equals(la.getPosition().getReferenceName())) {
				res += "=";
			} else {
				res += ra.getNextMatePosition().getReferenceName().toString();
			}
		} else {
			res += "";
		}
		res += FIELD_SEPARATOR;

		// mate position
		if (ra.getNextMatePosition() != null) {
			res += ("" + ra.getNextMatePosition().getPosition());
		} else {
			res += ("" + 0);
		}
		res += FIELD_SEPARATOR;

		// tlen
		res += ("" + ra.getFragmentLength());
		res += FIELD_SEPARATOR;

		// sequence
		res += ra.getAlignedSequence().toString();
		res += FIELD_SEPARATOR;

		// quality
		for(int v: ra.getAlignedQuality()) {
			res += (""  + (char) v);
		}

		// optional fields
		for (CharSequence key: ra.getInfo().keySet()) {
			res += FIELD_SEPARATOR;
			res += key.toString();
			for(CharSequence val: ra.getInfo().get(key)) {
				res += (":" + val.toString());
			}
		}
		
		return res;
	}
}
