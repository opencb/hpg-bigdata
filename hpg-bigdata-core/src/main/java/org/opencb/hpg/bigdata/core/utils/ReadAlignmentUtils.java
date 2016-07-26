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

package org.opencb.hpg.bigdata.core.utils;

import org.ga4gh.models.CigarUnit;
import org.ga4gh.models.LinearAlignment;
import org.ga4gh.models.ReadAlignment;
import org.ga4gh.models.Strand;

public class ReadAlignmentUtils {

    private static final String FIELD_SEPARATOR = "\t";

    protected ReadAlignmentUtils() {
    }

    public static String getSamString(ReadAlignment ra) {
        StringBuilder res = new StringBuilder();
        LinearAlignment la = (LinearAlignment) ra.getAlignment();

        // id
        res.append(ra.getId().toString()).append(FIELD_SEPARATOR);

        // flags
        int flags = 0;
        if (ra.getNumberReads() > 0) {
            flags |= 0x1;
        }
        // TODO Check this line, it was getProperPlacement() before
        if (!ra.getImproperPlacement()) {
            flags |= 0x2;
        }
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
        if (ra.getReadNumber() == 0) {
            flags |= 0x40;
        }
        if (ra.getNumberReads() > 0 && ra.getReadNumber() == ra.getNumberReads() - 1) {
            flags |= 0x80;
        }
        if (ra.getSecondaryAlignment()) {
            flags |= 0x100;
        }
        if (ra.getFailedVendorQualityChecks()) {
            flags |= 0x200;
        }
        if (ra.getDuplicateFragment()) {
            flags |= 0x400;
        }
        res.append(flags);
        res.append(FIELD_SEPARATOR);

        if (la == null) {
            res.append("*").append(FIELD_SEPARATOR);        // chromosome
            res.append("0").append(FIELD_SEPARATOR);        // position
            res.append("0").append(FIELD_SEPARATOR);        // mapping quality
            res.append(ra.getAlignedSequence().length()).append("M").append(FIELD_SEPARATOR);    // cigar
        } else {
            // chromosome
            res.append(la.getPosition().getReferenceName());
            res.append(FIELD_SEPARATOR);

            // position
            res.append(la.getPosition().getPosition() + 1); //0-based to 1-based
            res.append(FIELD_SEPARATOR);

            // mapping quality
            res.append(la.getMappingQuality());
            res.append(FIELD_SEPARATOR);

            // cigar
            for (CigarUnit e : la.getCigar()) {
                res.append(e.getOperationLength());
                switch (e.getOperation()) {
                    case ALIGNMENT_MATCH:
                        res.append("M");
                        break;
                    case INSERT:
                        res.append("I");
                        break;
                    case DELETE:
                        res.append("D");
                        break;
                    case SKIP:
                        res.append("N");
                        break;
                    case CLIP_SOFT:
                        res.append("S");
                        break;
                    case CLIP_HARD:
                        res.append("H");
                        break;
                    case PAD:
                        res.append("P");
                        break;
                    case SEQUENCE_MATCH:
                        res.append("=");
                        break;
                    case SEQUENCE_MISMATCH:
                        res.append("X");
                        break;
                    default:
                        break;
                }
            }
            res.append(FIELD_SEPARATOR);
        }

        // mate chromosome
        if (ra.getNextMatePosition() != null) {
            if (la != null && ra.getNextMatePosition().getReferenceName().equals(la.getPosition().getReferenceName())) {
                res.append("=");
            } else {
                res.append(ra.getNextMatePosition().getReferenceName());
            }
        } else {
            res.append("*");
        }
        res.append(FIELD_SEPARATOR);

        // mate position
        if (ra.getNextMatePosition() != null) {
            res.append(ra.getNextMatePosition().getPosition());
        } else {
            res.append(0);
        }
        res.append(FIELD_SEPARATOR);

        // tlen
        res.append(ra.getFragmentLength());
        res.append(FIELD_SEPARATOR);

        // sequence
        res.append(ra.getAlignedSequence().toString());
        res.append(FIELD_SEPARATOR);

        // quality
        for (int v: ra.getAlignedQuality()) {
            res.append((char) (v + 33)); // Add ASCII offset
        }

        // optional fields
        for (CharSequence key: ra.getInfo().keySet()) {
            res.append(FIELD_SEPARATOR);
            res.append(key.toString());
            for (CharSequence val : ra.getInfo().get(key)) {
                res.append((":" + val.toString()));
            }
        }

        return res.toString();
    }

    /**
     * Adjusts the quality value for optimized 8-level mapping quality scores.
     *
     * Quality range -> Mapped quality
     * 1     ->  1
     * 2-9   ->  6
     * 10-19 ->  15
     * 20-24 ->  22
     * 25-29 ->  27
     * 30-34 ->  33
     * 35-39 ->  27
     * >=40  ->  40
     *
     * Read more: http://www.illumina.com/documents/products/technotes/technote_understanding_quality_scores.pd
     *
     * @param quality original quality
     * @return Adjusted quality
     */
    public static int adjustQuality(int quality) {
        final int adjustedQuality;

        if (quality <= 1) {
            adjustedQuality = quality;
        } else {
            int qualRange = quality / 5;
            switch (qualRange) {
                case 0:
                case 1:
                    adjustedQuality = 6;
                    break;
                case 2:
                case 3:
                    adjustedQuality = 15;
                    break;
                case 4:
                    adjustedQuality = 22;
                    break;
                case 5:
                    adjustedQuality = 27;
                    break;
                case 6:
                    adjustedQuality = 33;
                    break;
                case 7:
                    adjustedQuality = 37;
                    break;
                case 8:
                default:
                    adjustedQuality = 40;
                    break;
            }
        }
        return adjustedQuality;
    }
}
