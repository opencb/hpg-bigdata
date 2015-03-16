package org.opencb.hpg.bigdata.core.converters;

import htsjdk.samtools.CigarElement;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecord.SAMTagAndValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ga4gh.models.CigarOperation;
import org.ga4gh.models.CigarUnit;
import org.ga4gh.models.LinearAlignment;
import org.ga4gh.models.Position;
import org.ga4gh.models.ReadAlignment;
import org.ga4gh.models.Strand;
import org.opencb.ga4gh.utils.ReadAlignmentUtils;

public class SAMRecord2ReadAlignmentConverter implements Converter<SAMRecord, ReadAlignment> {

	@Override
	public ReadAlignment forward(SAMRecord in) {
		//id
		CharSequence id = in.getReadName();

		// read group id
		CharSequence readGroupId;
		if (in.getReadGroup() != null) {
			readGroupId = in.getReadGroup().getId();
		} else {
			readGroupId = "no-group";
		}

		// reference name
		CharSequence fragmentName = in.getReferenceName();

		// the read is mapped in a proper pair
		boolean properPlacement = in.getReadPairedFlag() && in.getProperPairFlag();

		// the read is either a PCR duplicate or an optical duplicate.
		boolean duplicateFragment = in.getDuplicateReadFlag();

		// the number of reads in the fragment (extension to SAM flag 0x1)
		int numberReads = in.getProperPairFlag() ? 2 : 1;

		// the observed length of the fragment, equivalent to TLEN in SAM
		int fragmentLength = in.getReadPairedFlag() ? in.getInferredInsertSize() : 0;

		// The read number in sequencing. 0-based and less than numberReads. 
		// This field replaces SAM flag 0x40 and 0x80
		int readNumber = 0;
		if (in.getReadPairedFlag() && in.getSecondOfPairFlag()) {
			readNumber = numberReads - 1;
		}

		// the read fails platform/vendor quality checks
		boolean failedVendorQualityChecks = in.getReadFailsVendorQualityCheckFlag();

		// alignment
		Position position = new Position();
		position.setPosition((long) in.getAlignmentStart());
		position.setReferenceName(in.getReferenceName());
		position.setSequenceId("");
		position.setStrand(in.getReadNegativeStrandFlag() ? Strand.NEG_STRAND : Strand.POS_STRAND);
		int mappingQuality = in.getMappingQuality();
		List<CigarUnit> cigar = new ArrayList<CigarUnit>();
		for(CigarElement e: in.getCigar().getCigarElements()) {
			CigarOperation op;
			switch (e.getOperator()) {
			case M:
				op = CigarOperation.ALIGNMENT_MATCH;
				break;
			case I:
				op = CigarOperation.INSERT;
				break;
			case D:
				op = CigarOperation.DELETE;
				break;
			case N:
				op = CigarOperation.SKIP;
				break;
			case S:
				op = CigarOperation.CLIP_SOFT;
				break;
			case H:
				op = CigarOperation.CLIP_HARD;
				break;
			case P:
				op = CigarOperation.PAD;
				break;
			case EQ:
				op = CigarOperation.SEQUENCE_MATCH;
				break;
			case X:
				op = CigarOperation.SEQUENCE_MISMATCH;
				break;
			default:
				throw new IllegalArgumentException("Unrecognized CigarOperator: " + e);
			}
			cigar.add(new CigarUnit(op, (long) e.getLength(), null));
		}
		LinearAlignment alignment = new LinearAlignment(position, mappingQuality, cigar);

		// the read is the second read in a pair
		boolean secondaryAlignment = in.getSupplementaryAlignmentFlag();

		// the alignment is supplementary
		boolean supplementaryAlignment = in.getSupplementaryAlignmentFlag();

		// read sequence 
		CharSequence alignedSequence = in.getReadString();

		// aligned quality
		int size = in.getBaseQualityString().length();		
		List<Integer> alignedQuality = new ArrayList<Integer>(size);
		for(int i=0 ; i < size; i++) {
			alignedQuality.add((int) in.getBaseQualityString().charAt(i));
		}

		// next mate position
		Position nextMatePosition = new Position();
		if (in.getReadPairedFlag()) {	
			nextMatePosition.setPosition((long) in.getMateAlignmentStart());
			nextMatePosition.setReferenceName(in.getMateReferenceName());
			nextMatePosition.setSequenceId("");
			nextMatePosition.setStrand(in.getMateNegativeStrandFlag() ? Strand.NEG_STRAND : Strand.POS_STRAND);
		}

		// A map of additional read alignment information.
		Map<CharSequence, List<CharSequence>> info = new HashMap<CharSequence, List<CharSequence>>();
		List<SAMTagAndValue> attributes = in.getAttributes();
		for (SAMTagAndValue tv : attributes) {
			List<CharSequence> list = new ArrayList<CharSequence>();
			if (tv.value instanceof String) {
				list.add("Z");	
			} else if (tv.value instanceof Float) {
				list.add("f");	
			} else {
				list.add("i");
			}
			list.add("" + tv.value);
			info.put(tv.tag, list);
		}

		ReadAlignment out = new ReadAlignment(id, readGroupId, fragmentName, properPlacement, duplicateFragment, numberReads, 
				fragmentLength, readNumber, failedVendorQualityChecks, alignment, secondaryAlignment,
				supplementaryAlignment, alignedSequence, alignedQuality, nextMatePosition, info);

		return out;
	}

	@Override
	public SAMRecord backward(ReadAlignment in) {
		SAMRecord out = new SAMRecord(null);
		out.setReadName(in.getId().toString());
		return out;
	}

}
