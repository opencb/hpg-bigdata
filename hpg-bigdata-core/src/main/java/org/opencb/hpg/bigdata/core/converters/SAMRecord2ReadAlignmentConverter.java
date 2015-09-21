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

package org.opencb.hpg.bigdata.core.converters;

import htsjdk.samtools.CigarElement;
import htsjdk.samtools.SAMFormatException;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecord.SAMTagAndValue;
import htsjdk.samtools.TagValueAndUnsignedArrayFlag;
import htsjdk.samtools.TextTagCodec;
import htsjdk.samtools.util.StringUtil;

import java.util.*;

import org.ga4gh.models.CigarOperation;
import org.ga4gh.models.CigarUnit;
import org.ga4gh.models.LinearAlignment;
import org.ga4gh.models.Position;
import org.ga4gh.models.ReadAlignment;
import org.ga4gh.models.Strand;
import org.opencb.hpg.bigdata.core.utils.ReadAlignmentUtils;

public class SAMRecord2ReadAlignmentConverter implements Converter<SAMRecord, ReadAlignment> {

    // From SAM specification
    private static final int QNAME_COL = 0;
    private static final int FLAG_COL = 1;
    private static final int RNAME_COL = 2;
    private static final int POS_COL = 3;
    private static final int MAPQ_COL = 4;
    private static final int CIGAR_COL = 5;
    private static final int MRNM_COL = 6;
    private static final int MPOS_COL = 7;
    private static final int ISIZE_COL = 8;
    private static final int SEQ_COL = 9;
    private static final int QUAL_COL = 10;

    private static final int NUM_REQUIRED_FIELDS = 11;
    private final boolean adjustQuality;

    public SAMRecord2ReadAlignmentConverter() {
        adjustQuality = true;
    }

    public SAMRecord2ReadAlignmentConverter(boolean adjustQuality) {
        this.adjustQuality = adjustQuality;
    }


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
        int numberReads = in.getReadPairedFlag() ? 2 : 1;

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
        position.setPosition((long) in.getAlignmentStart() - 1); //from 1-based to 0-based
        position.setReferenceName(in.getReferenceName());
        position.setSequenceId("");
        position.setStrand(in.getReadNegativeStrandFlag() ? Strand.NEG_STRAND : Strand.POS_STRAND);
        int mappingQuality = in.getMappingQuality();
        List<CigarUnit> cigar = new ArrayList<CigarUnit>();
        for (CigarElement e: in.getCigar().getCigarElements()) {
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
        byte[] baseQualities = in.getBaseQualities();
        int size = baseQualities.length;
        List<Integer> alignedQuality = new ArrayList<>(size);
        if (adjustQuality) {
            for (byte baseQuality : baseQualities) {
                int adjustedQuality = ReadAlignmentUtils.adjustQuality(baseQuality);
                alignedQuality.add(adjustedQuality);
            }
        } else {
            for (byte baseQuality : baseQualities) {
                alignedQuality.add((int) baseQuality);
            }
        }

        // next mate position
        Position nextMatePosition = null;
        if (in.getReadPairedFlag()) {
            nextMatePosition = new Position();
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

        ReadAlignment out = new ReadAlignment(id, readGroupId, fragmentName, properPlacement, duplicateFragment,
                numberReads, fragmentLength, readNumber, failedVendorQualityChecks, alignment, secondaryAlignment,
                supplementaryAlignment, alignedSequence, alignedQuality, nextMatePosition, info);

        return out;
    }

    @Override
    public SAMRecord backward(ReadAlignment in) {

        final String samLine = ReadAlignmentUtils.getSamString(in);

        final String[] fields = new String[1000];
        final int numFields = StringUtil.split(samLine, fields, '\t');
        if (numFields < NUM_REQUIRED_FIELDS) {
            throw new IllegalArgumentException("Not enough fields");
        }
        if (numFields == fields.length) {
            throw new IllegalArgumentException("Too many fields in SAM text record.");
        }
        for (int i = 0; i < numFields; ++i) {
            if (fields[i].isEmpty()) {
                throw new IllegalArgumentException("Empty field at position " + i + " (zero-based)");
            }
        }

        SAMRecord out = new SAMRecord(null);

        out.setReadName(fields[QNAME_COL]);
        out.setFlags(Integer.valueOf(fields[FLAG_COL]));
        out.setReferenceName(fields[RNAME_COL]);
        out.setAlignmentStart(Integer.valueOf(fields[POS_COL]));
        out.setMappingQuality(Integer.valueOf(fields[MAPQ_COL]));
        out.setCigarString(fields[CIGAR_COL]);
        out.setMateReferenceName(fields[MRNM_COL].equals("=") ? out.getReferenceName() : fields[MRNM_COL]);
        out.setMateAlignmentStart(Integer.valueOf(fields[MPOS_COL]));
        out.setInferredInsertSize(Integer.valueOf(fields[ISIZE_COL]));
        if (!fields[SEQ_COL].equals("*")) {
            out.setReadString(fields[SEQ_COL]);
        } else {
            out.setReadBases(SAMRecord.NULL_SEQUENCE);
        }
        if (!fields[QUAL_COL].equals("*")) {
            out.setBaseQualityString(fields[QUAL_COL]);
        } else {
            out.setBaseQualities(SAMRecord.NULL_QUALS);
        }

        TextTagCodec tagCodec = new TextTagCodec();
        for (int i = NUM_REQUIRED_FIELDS; i < numFields; ++i) {
            Map.Entry<String, Object> entry = null;
            try {
                entry = tagCodec.decode(fields[i]);
            } catch (SAMFormatException e) {
                throw new IllegalArgumentException("Unable to decode field \"" + fields[i] + "\"", e);
            }
            if (entry != null) {
                if (entry.getValue() instanceof TagValueAndUnsignedArrayFlag) {
                    final TagValueAndUnsignedArrayFlag valueAndFlag =
                            (TagValueAndUnsignedArrayFlag) entry.getValue();
                    if (valueAndFlag.isUnsignedArray) {
                        out.setUnsignedArrayAttribute(entry.getKey(), valueAndFlag.value);
                    } else {
                        out.setAttribute(entry.getKey(), valueAndFlag.value);
                    }
                } else {
                    out.setAttribute(entry.getKey(), entry.getValue());
                }
            }
        }

        return out;
    }

}
