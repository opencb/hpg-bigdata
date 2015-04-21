/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <avro.h>
#include <stdio.h>
#include <stdlib.h>

#include "samtools/bam.h"

//--------------------------------------------------------------------//
// JSON schemas
//
// according to the ga4gh models (ga4gh.org)
//--------------------------------------------------------------------//

const char POSITION_SCHEMA[] = "{\"type\":\"record\",\"name\":\"Position\",\"namespace\":\"org.ga4gh.models\",\"doc\":\"A `Position` is a side of a base pair in some already known sequence. A\\n`Position` is represented by a sequence name or ID, a base number on that\\nsequence (0-based), and a `Strand` to indicate the left or right side.\\n\\nFor example, given the sequence \\\"GTGG\\\", the `Position` on that sequence at\\noffset 1 in the forward orientation would be the left side of the T/A base pair.\\nThe base at this `Position` is \\\"T\\\". Alternately, for offset 1 in the reverse\\norientation, the `Position` would be the right side of the T/A base pair, and\\nthe base at the `Position` is \\\"A\\\".\\n\\nOffsets added to a `Position` are interpreted as reading along its strand;\\nadding to a reverse strand position actually subtracts from its `position`\\nmember.\",\"fields\":[{\"name\":\"referenceName\",\"type\":[\"null\",\"string\"],\"doc\":\"The name of the reference sequence in whatever reference set is being used.\\n  Does not generally include a \\\"chr\\\" prefix, so for example \\\"X\\\" would be used\\n  for the X chromosome.\\n\\n  If `sequenceId` is null, this must not be null.\",\"default\":null},{\"name\":\"sequenceId\",\"type\":[\"null\",\"string\"],\"doc\":\"The ID of the sequence on which the `Position` is located. This may be a\\n  `Reference` sequence, or a novel piece of sequence associated with a\\n  `VariantSet`.\\n\\n  If `referenceName` is null, this must not be null.\\n\\n  If the server supports the \\\"graph\\\" mode, this must not be null.\",\"default\":null},{\"name\":\"position\",\"type\":\"long\",\"doc\":\"The 0-based offset from the start of the forward strand for that sequence.\\n  Genomic positions are non-negative integers less than sequence length.\"},{\"name\":\"strand\",\"type\":{\"type\":\"enum\",\"name\":\"Strand\",\"doc\":\"Indicates the DNA strand associate for some data item.\\n* `POS_STRAND`:  The postive (+) strand.\\n* `NEG_STRAND`: The negative (-) strand.\\n* `NO_STRAND`: Strand-independent data or data where the strand can not be determined.\",\"symbols\":[\"POS_STRAND\",\"NEG_STRAND\",\"NO_STRAND\"]},\"doc\":\"Strand the position is associated with. `POS_STRAND` represents the forward\\n  strand, or equivalently the left side of a base, and `NEG_STRAND` represents\\n  the reverse strand, or equivalently the right side of a base.\"}]}";

const char LINEAR_ALIGNMENT_SCHEMA0[] = "{\"type\":\"record\",\"name\":\"LinearAlignment\",\"namespace\":\"org.ga4gh.models\",\"fields\":[{\"name\":\"mappingQuality\",\"type\":[\"null\",\"int\"],\"doc\":\"The mapping quality of this alignment. Represents how likely\\n  the read maps to this position as opposed to other locations.\",\"default\":null}]}";

const char LINEAR_ALIGNMENT_SCHEMA[] = "{\"type\":\"record\",\"name\":\"LinearAlignment\",\"namespace\":\"org.ga4gh.models\",\"doc\":\"A linear alignment can be represented by one CIGAR string.\",\"fields\":[{\"name\":\"position\",\"type\":{\"type\":\"record\",\"name\":\"Position\",\"doc\":\"A `Position` is a side of a base pair in some already known sequence. A\\n`Position` is represented by a sequence name or ID, a base number on that\\nsequence (0-based), and a `Strand` to indicate the left or right side.\\n\\nFor example, given the sequence \\\"GTGG\\\", the `Position` on that sequence at\\noffset 1 in the forward orientation would be the left side of the T/A base pair.\\nThe base at this `Position` is \\\"T\\\". Alternately, for offset 1 in the reverse\\norientation, the `Position` would be the right side of the T/A base pair, and\\nthe base at the `Position` is \\\"A\\\".\\n\\nOffsets added to a `Position` are interpreted as reading along its strand;\\nadding to a reverse strand position actually subtracts from its `position`\\nmember.\",\"fields\":[{\"name\":\"referenceName\",\"type\":[\"null\",\"string\"],\"doc\":\"The name of the reference sequence in whatever reference set is being used.\\n  Does not generally include a \\\"chr\\\" prefix, so for example \\\"X\\\" would be used\\n  for the X chromosome.\\n\\n  If `sequenceId` is null, this must not be null.\",\"default\":null},{\"name\":\"sequenceId\",\"type\":[\"null\",\"string\"],\"doc\":\"The ID of the sequence on which the `Position` is located. This may be a\\n  `Reference` sequence, or a novel piece of sequence associated with a\\n  `VariantSet`.\\n\\n  If `referenceName` is null, this must not be null.\\n\\n  If the server supports the \\\"graph\\\" mode, this must not be null.\",\"default\":null},{\"name\":\"position\",\"type\":\"long\",\"doc\":\"The 0-based offset from the start of the forward strand for that sequence.\\n  Genomic positions are non-negative integers less than sequence length.\"},{\"name\":\"strand\",\"type\":{\"type\":\"enum\",\"name\":\"Strand\",\"doc\":\"Indicates the DNA strand associate for some data item.\\n* `POS_STRAND`:  The postive (+) strand.\\n* `NEG_STRAND`: The negative (-) strand.\\n* `NO_STRAND`: Strand-independent data or data where the strand can not be determined.\",\"symbols\":[\"POS_STRAND\",\"NEG_STRAND\",\"NO_STRAND\"]},\"doc\":\"Strand the position is associated with. `POS_STRAND` represents the forward\\n  strand, or equivalently the left side of a base, and `NEG_STRAND` represents\\n  the reverse strand, or equivalently the right side of a base.\"}]},\"doc\":\"The position of this alignment.\"},{\"name\":\"mappingQuality\",\"type\":[\"null\",\"int\"],\"doc\":\"The mapping quality of this alignment. Represents how likely\\n  the read maps to this position as opposed to other locations.\",\"default\":null},{\"name\":\"cigar\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"CigarUnit\",\"doc\":\"A structure for an instance of a CIGAR operation.\",\"fields\":[{\"name\":\"operation\",\"type\":{\"type\":\"enum\",\"name\":\"CigarOperation\",\"doc\":\"An enum for the different types of CIGAR alignment operations that exist.\\nUsed wherever CIGAR alignments are used. The different enumerated values\\nhave the following usage:\\n\\n* `ALIGNMENT_MATCH`: An alignment match indicates that a sequence can be\\n  aligned to the reference without evidence of an INDEL. Unlike the\\n  `SEQUENCE_MATCH` and `SEQUENCE_MISMATCH` operators, the `ALIGNMENT_MATCH`\\n  operator does not indicate whether the reference and read sequences are an\\n  exact match. This operator is equivalent to SAM's `M`.\\n* `INSERT`: The insert operator indicates that the read contains evidence of\\n  bases being inserted into the reference. This operator is equivalent to\\n  SAM's `I`.\\n* `DELETE`: The delete operator indicates that the read contains evidence of\\n  bases being deleted from the reference. This operator is equivalent to\\n  SAM's `D`.\\n* `SKIP`: The skip operator indicates that this read skips a long segment of\\n  the reference, but the bases have not been deleted. This operator is\\n  commonly used when working with RNA-seq data, where reads may skip long\\n  segments of the reference between exons. This operator is equivalent to\\n  SAM's 'N'.\\n* `CLIP_SOFT`: The soft clip operator indicates that bases at the start/end\\n  of a read have not been considered during alignment. This may occur if the\\n  majority of a read maps, except for low quality bases at the start/end of\\n  a read. This operator is equivalent to SAM's 'S'. Bases that are soft clipped\\n  will still be stored in the read.\\n* `CLIP_HARD`: The hard clip operator indicates that bases at the start/end of\\n  a read have been omitted from this alignment. This may occur if this linear\\n  alignment is part of a chimeric alignment, or if the read has been trimmed\\n  (e.g., during error correction, or to trim poly-A tails for RNA-seq). This\\n  operator is equivalent to SAM's 'H'.\\n* `PAD`: The pad operator indicates that there is padding in an alignment.\\n  This operator is equivalent to SAM's 'P'.\\n* `SEQUENCE_MATCH`: This operator indicates that this portion of the aligned\\n  sequence exactly matches the reference (e.g., all bases are equal to the\\n  reference bases). This operator is equivalent to SAM's '='.\\n* `SEQUENCE_MISMATCH`: This operator indicates that this portion of the\\n  aligned sequence is an alignment match to the reference, but a sequence\\n  mismatch (e.g., the bases are not equal to the reference). This can\\n  indicate a SNP or a read error. This operator is equivalent to SAM's 'X'.\",\"symbols\":[\"ALIGNMENT_MATCH\",\"INSERT\",\"DELETE\",\"SKIP\",\"CLIP_SOFT\",\"CLIP_HARD\",\"PAD\",\"SEQUENCE_MATCH\",\"SEQUENCE_MISMATCH\"]},\"doc\":\"The operation type.\"},{\"name\":\"operationLength\",\"type\":\"long\",\"doc\":\"The number of bases that the operation runs for.\"},{\"name\":\"referenceSequence\",\"type\":[\"null\",\"string\"],\"doc\":\"`referenceSequence` is only used at mismatches (`SEQUENCE_MISMATCH`)\\n  and deletions (`DELETE`). Filling this field replaces the MD tag.\\n  If the relevant information is not available, leave this field as `null`.\",\"default\":null}]}},\"doc\":\"Represents the local alignment of this sequence (alignment matches, indels, etc)\\n  versus the reference.\",\"default\":[]}]}";


const char READ_ALIGNMENT_SCHEMA[] = "{\"type\":\"record\",\"name\":\"ReadAlignment\",\"namespace\":\"org.ga4gh.models\",\"doc\":\"Each read alignment describes an alignment with additional information\\nabout the fragment and the read. A read alignment object is equivalent to a\\nline in a SAM file.\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"string\"],\"doc\":\"The read alignment ID. This ID is unique within the read group this\\n  alignment belongs to. This field may not be provided by all backends.\\n  Its intended use is to make caching and UI display easier for\\n  genome browsers and other light weight clients.\"},{\"name\":\"readGroupId\",\"type\":\"string\",\"doc\":\"The ID of the read group this read belongs to.\\n  (Every read must belong to exactly one read group.)\"},{\"name\":\"fragmentName\",\"type\":\"string\",\"doc\":\"The fragment name. Equivalent to QNAME (query template name) in SAM.\"},{\"name\":\"properPlacement\",\"type\":[\"boolean\",\"null\"],\"doc\":\"The orientation and the distance between reads from the fragment are\\n  consistent with the sequencing protocol (equivalent to SAM flag 0x2)\",\"default\":false},{\"name\":\"duplicateFragment\",\"type\":[\"boolean\",\"null\"],\"doc\":\"The fragment is a PCR or optical duplicate (SAM flag 0x400)\",\"default\":false},{\"name\":\"numberReads\",\"type\":[\"null\",\"int\"],\"doc\":\"The number of reads in the fragment (extension to SAM flag 0x1)\",\"default\":null},{\"name\":\"fragmentLength\",\"type\":[\"null\",\"int\"],\"doc\":\"The observed length of the fragment, equivalent to TLEN in SAM.\",\"default\":null},{\"name\":\"readNumber\",\"type\":[\"null\",\"int\"],\"doc\":\"The read number in sequencing. 0-based and less than numberReads. This field\\n  replaces SAM flag 0x40 and 0x80.\",\"default\":null},{\"name\":\"failedVendorQualityChecks\",\"type\":[\"boolean\",\"null\"],\"doc\":\"SAM flag 0x200\",\"default\":false},{\"name\":\"alignment\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"LinearAlignment\",\"doc\":\"A linear alignment can be represented by one CIGAR string.\",\"fields\":[{\"name\":\"position\",\"type\":{\"type\":\"record\",\"name\":\"Position\",\"doc\":\"A `Position` is a side of a base pair in some already known sequence. A\\n`Position` is represented by a sequence name or ID, a base number on that\\nsequence (0-based), and a `Strand` to indicate the left or right side.\\n\\nFor example, given the sequence \\\"GTGG\\\", the `Position` on that sequence at\\noffset 1 in the forward orientation would be the left side of the T/A base pair.\\nThe base at this `Position` is \\\"T\\\". Alternately, for offset 1 in the reverse\\norientation, the `Position` would be the right side of the T/A base pair, and\\nthe base at the `Position` is \\\"A\\\".\\n\\nOffsets added to a `Position` are interpreted as reading along its strand;\\nadding to a reverse strand position actually subtracts from its `position`\\nmember.\",\"fields\":[{\"name\":\"referenceName\",\"type\":[\"null\",\"string\"],\"doc\":\"The name of the reference sequence in whatever reference set is being used.\\n  Does not generally include a \\\"chr\\\" prefix, so for example \\\"X\\\" would be used\\n  for the X chromosome.\\n\\n  If `sequenceId` is null, this must not be null.\",\"default\":null},{\"name\":\"sequenceId\",\"type\":[\"null\",\"string\"],\"doc\":\"The ID of the sequence on which the `Position` is located. This may be a\\n  `Reference` sequence, or a novel piece of sequence associated with a\\n  `VariantSet`.\\n\\n  If `referenceName` is null, this must not be null.\\n\\n  If the server supports the \\\"graph\\\" mode, this must not be null.\",\"default\":null},{\"name\":\"position\",\"type\":\"long\",\"doc\":\"The 0-based offset from the start of the forward strand for that sequence.\\n  Genomic positions are non-negative integers less than sequence length.\"},{\"name\":\"strand\",\"type\":{\"type\":\"enum\",\"name\":\"Strand\",\"doc\":\"Indicates the DNA strand associate for some data item.\\n* `POS_STRAND`:  The postive (+) strand.\\n* `NEG_STRAND`: The negative (-) strand.\\n* `NO_STRAND`: Strand-independent data or data where the strand can not be determined.\",\"symbols\":[\"POS_STRAND\",\"NEG_STRAND\",\"NO_STRAND\"]},\"doc\":\"Strand the position is associated with. `POS_STRAND` represents the forward\\n  strand, or equivalently the left side of a base, and `NEG_STRAND` represents\\n  the reverse strand, or equivalently the right side of a base.\"}]},\"doc\":\"The position of this alignment.\"},{\"name\":\"mappingQuality\",\"type\":[\"null\",\"int\"],\"doc\":\"The mapping quality of this alignment. Represents how likely\\n  the read maps to this position as opposed to other locations.\",\"default\":null},{\"name\":\"cigar\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"CigarUnit\",\"doc\":\"A structure for an instance of a CIGAR operation.\",\"fields\":[{\"name\":\"operation\",\"type\":{\"type\":\"enum\",\"name\":\"CigarOperation\",\"doc\":\"An enum for the different types of CIGAR alignment operations that exist.\\nUsed wherever CIGAR alignments are used. The different enumerated values\\nhave the following usage:\\n\\n* `ALIGNMENT_MATCH`: An alignment match indicates that a sequence can be\\n  aligned to the reference without evidence of an INDEL. Unlike the\\n  `SEQUENCE_MATCH` and `SEQUENCE_MISMATCH` operators, the `ALIGNMENT_MATCH`\\n  operator does not indicate whether the reference and read sequences are an\\n  exact match. This operator is equivalent to SAM's `M`.\\n* `INSERT`: The insert operator indicates that the read contains evidence of\\n  bases being inserted into the reference. This operator is equivalent to\\n  SAM's `I`.\\n* `DELETE`: The delete operator indicates that the read contains evidence of\\n  bases being deleted from the reference. This operator is equivalent to\\n  SAM's `D`.\\n* `SKIP`: The skip operator indicates that this read skips a long segment of\\n  the reference, but the bases have not been deleted. This operator is\\n  commonly used when working with RNA-seq data, where reads may skip long\\n  segments of the reference between exons. This operator is equivalent to\\n  SAM's 'N'.\\n* `CLIP_SOFT`: The soft clip operator indicates that bases at the start/end\\n  of a read have not been considered during alignment. This may occur if the\\n  majority of a read maps, except for low quality bases at the start/end of\\n  a read. This operator is equivalent to SAM's 'S'. Bases that are soft clipped\\n  will still be stored in the read.\\n* `CLIP_HARD`: The hard clip operator indicates that bases at the start/end of\\n  a read have been omitted from this alignment. This may occur if this linear\\n  alignment is part of a chimeric alignment, or if the read has been trimmed\\n  (e.g., during error correction, or to trim poly-A tails for RNA-seq). This\\n  operator is equivalent to SAM's 'H'.\\n* `PAD`: The pad operator indicates that there is padding in an alignment.\\n  This operator is equivalent to SAM's 'P'.\\n* `SEQUENCE_MATCH`: This operator indicates that this portion of the aligned\\n  sequence exactly matches the reference (e.g., all bases are equal to the\\n  reference bases). This operator is equivalent to SAM's '='.\\n* `SEQUENCE_MISMATCH`: This operator indicates that this portion of the\\n  aligned sequence is an alignment match to the reference, but a sequence\\n  mismatch (e.g., the bases are not equal to the reference). This can\\n  indicate a SNP or a read error. This operator is equivalent to SAM's 'X'.\",\"symbols\":[\"ALIGNMENT_MATCH\",\"INSERT\",\"DELETE\",\"SKIP\",\"CLIP_SOFT\",\"CLIP_HARD\",\"PAD\",\"SEQUENCE_MATCH\",\"SEQUENCE_MISMATCH\"]},\"doc\":\"The operation type.\"},{\"name\":\"operationLength\",\"type\":\"long\",\"doc\":\"The number of bases that the operation runs for.\"},{\"name\":\"referenceSequence\",\"type\":[\"null\",\"string\"],\"doc\":\"`referenceSequence` is only used at mismatches (`SEQUENCE_MISMATCH`)\\n  and deletions (`DELETE`). Filling this field replaces the MD tag.\\n  If the relevant information is not available, leave this field as `null`.\",\"default\":null}]}},\"doc\":\"Represents the local alignment of this sequence (alignment matches, indels, etc)\\n  versus the reference.\",\"default\":[]}]},{\"type\":\"record\",\"name\":\"GraphAlignment\",\"doc\":\"A string-to-reference-graph alignment can be represented by one CIGAR string and\\none `Path` through multiple `Reference`s, against which the CIGAR string is\\ninterpreted.\\n\\nNote that `Path`s in `GraphAlignment`s are restricted to visiting `Reference`s\\nand following reference adjacencies. If a read needs to be aligned to sequences\\nthat are not present in a `ReferenceSet`, it needs to be aligned to a new\\n`ReferenceSet` with those sequences. If a read needs to follow adjacencies that\\nare not present in the `ReferenceSet` it's being aligned to, it should be\\nrepresented as a \\\"chimeric\\\" alignment, and should use multiple `ReadAlignment`s\\nand the supplementaryAlignment flag instead of a single `GraphAlignment`.\\n\\nSome especially large deletions could be represented just as well as a large\\ndeletion in the CIGAR string, or as a chimeric alignment.\",\"fields\":[{\"name\":\"path\",\"type\":{\"type\":\"record\",\"name\":\"Path\",\"doc\":\"A `Path` is an ordered list of `Segment`s. In general any contiguous path\\nthrough a sequence graph, with no novel adjacencies, can be represented by a\\n`Path`.\",\"fields\":[{\"name\":\"segments\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Segment\",\"doc\":\"A `Segment` is a range on a sequence, possibly including the joins at the\\nsequence's ends. It does not include base data. (The bases for a sequence are\\navailable through the `getSequenceBases()` API call.)\\n\\nIn the sequence \\\"GTGG\\\", the segment starting at index 1 on the forward strand\\nwith length 2 is the \\\"TG\\\" on the forward strand. The length-2 segment starting\\nat index 1 on the reverse strand is \\\"AC\\\", corresponding to the first two base\\npairs of the sequence, or the last two bases of the reverse complement.\",\"fields\":[{\"name\":\"start\",\"type\":\"Position\",\"doc\":\"The sequence ID and start index of this `Segment`. This base is always\\n  included in the segment, regardless of orientation.\"},{\"name\":\"length\",\"type\":\"long\",\"doc\":\"The length of this `Segment`'s sequence. If `start` is on the forward strand,\\n  the `Segment` contains the range [`start.position`, `start.position` +\\n  `length`). If `start` is on the reverse strand, the `Segment` contains the\\n  range (`start.position` - `length`, `start.position`]. This is equivalent to\\n  starting from the side indicated by `start`, and traversing through that base\\n  out to the specified length.\"},{\"name\":\"startJoin\",\"type\":[\"null\",\"Position\"],\"doc\":\"Start and end `Position`s where this `Segment` attaches to other sequences.\\n  Note that the segmentId for start and end might not be the same. The\\n  `Segment`s covering the sequences onto which this `Segment` is joined are\\n  called its \\\"parents\\\", while this segment is a \\\"child\\\".\\n\\n  Joins may occur on the outer sides of the terminal bases in a sequence: the\\n  left side of the base at index 0, and the right side of the base with maximum\\n  index. These are the \\\"terminal sides\\\" of the sequence. `startJoin` is the join\\n  on the side indicated by `start`, and may only be set if that side is a\\n  terminal side. Similarly, `endJoin` is the join on the opposite side of the\\n  piece of sequence selected by the segment, and may only be set if that side is\\n  a terminal side. The value of `startJoin` or `endJoin`, if set, is the side to\\n  which the corresponding side of this `Sequence` is connected.\",\"default\":null},{\"name\":\"endJoin\",\"type\":[\"null\",\"Position\"],\"default\":null}]}},\"doc\":\"We require that one of each consecutive pair of `Segment`s in a `Path` be\\n  joined onto the other. `Segment`s appear in the order in which they occur when\\n  walking the path from one end to the other.\",\"default\":[]}]},\"doc\":\"The `Path` against which the read is aligned\"},{\"name\":\"mappingQuality\",\"type\":[\"null\",\"int\"],\"doc\":\"The mapping quality of this alignment. Represents how likely\\n  the read maps to this position as opposed to other locations.\",\"default\":null},{\"name\":\"cigar\",\"type\":{\"type\":\"array\",\"items\":\"CigarUnit\"},\"doc\":\"Represents the local alignment of this sequence (alignment matches, indels,\\n  etc) versus the `Path`.\",\"default\":[]}]}],\"doc\":\"The alignment for this alignment record. This field will be\\n  null if the read is unmapped.\\n\\n  If an API server supports \\\"classic\\\" mode, it must not return `GraphAlignment`\\n  objects here. If the API server supports the \\\"graph\\\" mode and does not support\\n  the \\\"classic\\\" mode, it must not return `LinearAlignment` objects here.\",\"default\":null},{\"name\":\"secondaryAlignment\",\"type\":[\"boolean\",\"null\"],\"doc\":\"Whether this alignment is secondary. Equivalent to SAM flag 0x100.\\n  A secondary alignment represents an alternative to the primary alignment\\n  for this read. Aligners may return secondary alignments if a read can map\\n  ambiguously to multiple coordinates in the genome.\\n\\n  By convention, each read has one and only one alignment where both\\n  secondaryAlignment and supplementaryAlignment are false.\",\"default\":false},{\"name\":\"supplementaryAlignment\",\"type\":[\"boolean\",\"null\"],\"doc\":\"Whether this alignment is supplementary. Equivalent to SAM flag 0x800.\\n  Supplementary alignments are used in the representation of a chimeric\\n  alignment, which follows nonreference adjacencies not describable as indels.\\n  In a chimeric alignment, a read is split into multiple alignments that\\n  may map to different reference contigs. The first alignment in the read will\\n  be designated as the representative alignment; the remaining alignments will\\n  be designated as supplementary alignments. These alignments may have different\\n  mapping quality scores.\\n\\n  In each alignment in a chimeric alignment, the read will be hard clipped. The\\n  `alignedSequence` and `alignedQuality` fields in the alignment record will\\n  only represent the bases for its respective alignment.\",\"default\":false},{\"name\":\"alignedSequence\",\"type\":[\"null\",\"string\"],\"doc\":\"The bases of the read sequence contained in this alignment record.\\n  `alignedSequence` and `alignedQuality` may be shorter than the full read sequence\\n  and quality. This will occur if the alignment is part of a chimeric alignment,\\n  or if the read was trimmed. When this occurs, the CIGAR for this read will\\n  begin/end with a hard clip operator that will indicate the length of the excised sequence.\",\"default\":null},{\"name\":\"alignedQuality\",\"type\":{\"type\":\"array\",\"items\":\"int\"},\"doc\":\"The quality of the read sequence contained in this alignment record.\\n  `alignedSequence` and `alignedQuality` may be shorter than the full read sequence\\n  and quality. This will occur if the alignment is part of a chimeric alignment,\\n  or if the read was trimmed. When this occurs, the CIGAR for this read will\\n  begin/end with a hard clip operator that will indicate the length of the excised sequence.\",\"default\":[]},{\"name\":\"nextMatePosition\",\"type\":[\"null\",\"Position\"],\"doc\":\"The mapping of the primary alignment of the `(readNumber+1)%numberReads`\\n  read in the fragment. It replaces mate position and mate strand in SAM.\",\"default\":null},{\"name\":\"info\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"array\",\"items\":\"string\"}},\"doc\":\"A map of additional read alignment information.\",\"default\":{}}]}";


const char CIGAR_UNIT_SCHEMA[] = "{\"type\":\"record\",\"name\":\"CigarUnit\",\"namespace\":\"org.ga4gh.models\",\"doc\":\"A structure for an instance of a CIGAR operation.\",\"fields\":[{\"name\":\"operation\",\"type\":{\"type\":\"enum\",\"name\":\"CigarOperation\",\"doc\":\"An enum for the different types of CIGAR alignment operations that exist.\\nUsed wherever CIGAR alignments are used. The different enumerated values\\nhave the following usage:\\n\\n* `ALIGNMENT_MATCH`: An alignment match indicates that a sequence can be\\n  aligned to the reference without evidence of an INDEL. Unlike the\\n  `SEQUENCE_MATCH` and `SEQUENCE_MISMATCH` operators, the `ALIGNMENT_MATCH`\\n  operator does not indicate whether the reference and read sequences are an\\n  exact match. This operator is equivalent to SAM's `M`.\\n* `INSERT`: The insert operator indicates that the read contains evidence of\\n  bases being inserted into the reference. This operator is equivalent to\\n  SAM's `I`.\\n* `DELETE`: The delete operator indicates that the read contains evidence of\\n  bases being deleted from the reference. This operator is equivalent to\\n  SAM's `D`.\\n* `SKIP`: The skip operator indicates that this read skips a long segment of\\n  the reference, but the bases have not been deleted. This operator is\\n  commonly used when working with RNA-seq data, where reads may skip long\\n  segments of the reference between exons. This operator is equivalent to\\n  SAM's 'N'.\\n* `CLIP_SOFT`: The soft clip operator indicates that bases at the start/end\\n  of a read have not been considered during alignment. This may occur if the\\n  majority of a read maps, except for low quality bases at the start/end of\\n  a read. This operator is equivalent to SAM's 'S'. Bases that are soft clipped\\n  will still be stored in the read.\\n* `CLIP_HARD`: The hard clip operator indicates that bases at the start/end of\\n  a read have been omitted from this alignment. This may occur if this linear\\n  alignment is part of a chimeric alignment, or if the read has been trimmed\\n  (e.g., during error correction, or to trim poly-A tails for RNA-seq). This\\n  operator is equivalent to SAM's 'H'.\\n* `PAD`: The pad operator indicates that there is padding in an alignment.\\n  This operator is equivalent to SAM's 'P'.\\n* `SEQUENCE_MATCH`: This operator indicates that this portion of the aligned\\n  sequence exactly matches the reference (e.g., all bases are equal to the\\n  reference bases). This operator is equivalent to SAM's '='.\\n* `SEQUENCE_MISMATCH`: This operator indicates that this portion of the\\n  aligned sequence is an alignment match to the reference, but a sequence\\n  mismatch (e.g., the bases are not equal to the reference). This can\\n  indicate a SNP or a read error. This operator is equivalent to SAM's 'X'.\",\"symbols\":[\"ALIGNMENT_MATCH\",\"INSERT\",\"DELETE\",\"SKIP\",\"CLIP_SOFT\",\"CLIP_HARD\",\"PAD\",\"SEQUENCE_MATCH\",\"SEQUENCE_MISMATCH\"]},\"doc\":\"The operation type.\"},{\"name\":\"operationLength\",\"type\":\"long\",\"doc\":\"The number of bases that the operation runs for.\"},{\"name\":\"referenceSequence\",\"type\":[\"null\",\"string\"],\"doc\":\"`referenceSequence` is only used at mismatches (`SEQUENCE_MISMATCH`)\\n  and deletions (`DELETE`). Filling this field replaces the MD tag.\\n  If the relevant information is not available, leave this field as `null`.\",\"default\":null}]}";

//--------------------------------------------------------------------//
// global variables
//--------------------------------------------------------------------//

#define MAX_BUFFER_SIZE  4098
char buffer[MAX_BUFFER_SIZE];

avro_schema_t read_alignment_schema;
avro_schema_t position_schema;
avro_schema_t linear_alignment_schema;
avro_schema_t cigar_unit_schema;


avro_schema_t union_schema_ns;
avro_value_iface_t *iface_union_schema_ns;
avro_value_t union_schema_ns_value;

avro_schema_t union_schema_bn;
avro_schema_t union_schema_ni;
avro_schema_t union_schema_na;
avro_schema_t union_schema_np;

// read alignment
avro_value_iface_t *iface_read_alignment;
avro_value_t read_alignment;

// position
avro_value_iface_t *iface_position;
avro_value_t position;

//--------------------------------------------------------------------//
// init schemas
//
// parse schema into a schema data structure
//--------------------------------------------------------------------//

void init_schemas() {
	int error;

	// read alignment
	if (error = avro_schema_from_json_literal(READ_ALIGNMENT_SCHEMA, &read_alignment_schema)) {
		fprintf(stderr, "\n\n%s\n\n", READ_ALIGNMENT_SCHEMA);
		fprintf(stderr, "Unable to parse read alignment schema (error %i)\n", error);
		fprintf(stderr, "Error message: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}
	iface_read_alignment = avro_generic_class_from_schema(read_alignment_schema);
	avro_generic_value_new(iface_read_alignment, &read_alignment);

	// position
	if (error = avro_schema_from_json_literal(POSITION_SCHEMA, &position_schema)) {
		fprintf(stderr, "\n\n%s\n\n", POSITION_SCHEMA);
		fprintf(stderr, "Unable to parse position schema (error %i)\n", error);
		fprintf(stderr, "Error message: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}
	iface_position = avro_generic_class_from_schema(position_schema);
	avro_generic_value_new(iface_position, &position);

	if (error = avro_schema_from_json_literal(LINEAR_ALIGNMENT_SCHEMA, &linear_alignment_schema)) {
		fprintf(stderr, "\n\n%s\n\n", LINEAR_ALIGNMENT_SCHEMA);
		fprintf(stderr, "Unable to parse linear alignment schema (error %i)\n", error);
		fprintf(stderr, "Error message: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}

	if (error = avro_schema_from_json_literal(CIGAR_UNIT_SCHEMA, &cigar_unit_schema)) {
		fprintf(stderr, "\n\n%s\n\n", CIGAR_UNIT_SCHEMA);
		fprintf(stderr, "Unable to parse cigar unit schema (error %i)\n", error);
		fprintf(stderr, "Error message: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}

	union_schema_ns = avro_schema_union();
	avro_schema_union_append(union_schema_ns, avro_schema_null());
	avro_schema_union_append(union_schema_ns, avro_schema_string());
	iface_union_schema_ns = avro_generic_class_from_schema(union_schema_ns);
	avro_generic_value_new(iface_union_schema_ns, &union_schema_ns_value);
	//  avro_generic_value_new(iface_read_alignment, &read_alignment);

	union_schema_bn = avro_schema_union();
	avro_schema_union_append(union_schema_bn, avro_schema_boolean());
	avro_schema_union_append(union_schema_bn, avro_schema_null());

	union_schema_ni = avro_schema_union();
	avro_schema_union_append(union_schema_ni, avro_schema_null());
	avro_schema_union_append(union_schema_ni, avro_schema_int());

	union_schema_na = avro_schema_union();
	avro_schema_union_append(union_schema_na, avro_schema_null());

	union_schema_np = avro_schema_union();
	avro_schema_union_append(union_schema_np, avro_schema_null());
	avro_schema_union_append(union_schema_np, avro_schema_null());
}

//--------------------------------------------------------------------//
// free schemas
//--------------------------------------------------------------------//

void free_schemas() {
	// read alignment
	avro_schema_decref(read_alignment_schema);
	avro_value_decref(&read_alignment);
	avro_value_iface_decref(iface_read_alignment);

	// position
	avro_schema_decref(position_schema);
	avro_value_decref(&position);
	avro_value_iface_decref(iface_position);

	avro_schema_decref(linear_alignment_schema);
	avro_schema_decref(cigar_unit_schema);

	// union [null, string]
	avro_schema_decref(union_schema_ns);
	avro_value_iface_decref(iface_union_schema_ns);
	avro_value_decref(&union_schema_ns_value);

	avro_schema_decref(union_schema_bn);
	avro_schema_decref(union_schema_ni);
	avro_schema_decref(union_schema_na);
	avro_schema_decref(union_schema_np);
}

//--------------------------------------------------------------------//
// add read alignment
//
// new Avro API (avro_value_xxx)
//--------------------------------------------------------------------//

void add_read_alignment2(avro_file_writer_t db, const bam1_t *bam1, const bam_header_t *bam_header) {
	int aux;
	avro_value_t field, subfield, subsubfield;
	avro_value_t element, subelement, branch, subbranch;

	// reset
	avro_value_reset(&read_alignment);

	// 0 id
	//avro_value_get_by_name(&read_alignment, "id", &field, NULL);
	avro_value_get_by_index(&read_alignment, 0, &field, NULL);
	avro_value_set_branch(&field, 1, &branch);
	avro_value_set_string(&branch, bam1_qname(bam1));

	// 1 readGroupId
	//avro_value_get_by_name(&read_alignment, "readGroupId", &field, NULL);
	avro_value_get_by_index(&read_alignment, 1, &field, NULL);
	avro_value_set_string(&field, "read-group-id-value");

	// 2 fragmentName
	//avro_value_get_by_name(&read_alignment, "fragmentName", &field, NULL);
	avro_value_get_by_index(&read_alignment, 2, &field, NULL);
	avro_value_set_string(&field, bam_header->target_name[bam1->core.tid]);

	// 3 properPlacement
	aux = ((bam1->core.flag & BAM_FPROPER_PAIR) && (bam1->core.flag & BAM_FPAIRED));
	//avro_value_get_by_name(&read_alignment, "properPlacement", &field, NULL);
	avro_value_get_by_index(&read_alignment, 3, &field, NULL);
	avro_value_set_branch(&field, 0, &branch);
	avro_value_set_boolean(&branch, aux);

	// 4 duplicateFragment
	aux = ((bam1->core.flag & BAM_FDUP) == BAM_FDUP);
	//avro_value_get_by_name(&read_alignment, "duplicateFragment", &field, NULL);
	avro_value_get_by_index(&read_alignment, 4, &field, NULL);
	avro_value_set_branch(&field, 0, &branch);
	avro_value_set_boolean(&branch, aux);

	// 5 numberReads
	int num_reads = (bam1->core.flag & BAM_FPROPER_PAIR) ? 2 : 1;
	//avro_value_get_by_name(&read_alignment, "numberReads", &field, NULL);
	avro_value_get_by_index(&read_alignment, 5, &field, NULL);
	avro_value_set_branch(&field, 1, &branch);
	avro_value_set_int(&branch, num_reads);

	// 6 fragmentLength
	aux = (bam1->core.flag & BAM_FPAIRED) ? bam1->core.isize : 0;
	//avro_value_get_by_name(&read_alignment, "fragmentLength", &field, NULL);
	avro_value_get_by_index(&read_alignment, 6, &field, NULL);
	avro_value_set_branch(&field, 1, &branch);
	avro_value_set_int(&branch, aux);

	// 7 readNumber
	aux = ((bam1->core.flag & BAM_FPAIRED) && (bam1->core.flag & BAM_FREAD2)) ? num_reads - 1 : 0;
	//avro_value_get_by_name(&read_alignment, "readNumber", &field, NULL);
	avro_value_get_by_index(&read_alignment, 7, &field, NULL);
	avro_value_set_branch(&field, 1, &branch);
	avro_value_set_int(&branch, aux);

	// 8 failedVendorQualityChecks
	aux = ((bam1->core.flag & BAM_FQCFAIL) == BAM_FQCFAIL);
	//avro_value_get_by_name(&read_alignment, "failedVendorQualityChecks", &field, NULL);
	avro_value_get_by_index(&read_alignment, 8, &field, NULL);
	avro_value_set_branch(&field, 0, &branch);
	avro_value_set_boolean(&branch, aux);

	// 9 alignment
	//avro_value_get_by_name(&read_alignment, "alignment", &field, NULL);
	avro_value_get_by_index(&read_alignment, 9, &field, NULL);
	if (bam1->core.flag & BAM_FUNMAP) {
		// unmapped
		avro_value_set_branch(&field, 0, &branch);
		avro_value_set_null(&branch);
	} else {
		// linear alignment
		avro_value_set_branch(&field, 1, &branch);

		// alignment . 0 position
		//avro_value_get_by_name(&branch, "position", &subfield, NULL);
		avro_value_get_by_index(&branch, 0, &subfield, NULL);

		// alignment . position . 0 referenceName
		//avro_value_get_by_name(&subfield, "referenceName", &subsubfield, NULL);
		avro_value_get_by_index(&subfield, 0, &subsubfield, NULL);
		avro_value_set_branch(&subsubfield, 1, &subbranch);
		avro_value_set_string(&subbranch, bam_header->target_name[bam1->core.tid]);

		// alignment . position . 1 sequenceId
		//avro_value_get_by_name(&subfield, "sequenceId", &subsubfield, NULL);
		avro_value_get_by_index(&subfield, 1, &subsubfield, NULL);
		avro_value_set_branch(&subsubfield, 0, &subbranch);
		avro_value_set_null(&subbranch);

		// alignment . position . 2 position
		//avro_value_get_by_name(&subfield, "position", &subsubfield, NULL);
		avro_value_get_by_index(&subfield, 2, &subsubfield, NULL);
		avro_value_set_long(&subsubfield, bam1->core.pos);

		// alignment . position . 3 strand
		//avro_value_get_by_name(&subfield, "strand", &subsubfield, NULL);
		avro_value_get_by_index(&subfield, 3, &subsubfield, NULL);
		if (bam1->core.flag & BAM_FREVERSE) {
			avro_value_set_enum(&subsubfield, 1);
		} else {
			avro_value_set_enum(&subsubfield, 0);
		}

		// alignment . 1 mappingQuaility
		//avro_value_get_by_name(&branch, "mappingQuality", &subfield, NULL);
		avro_value_get_by_index(&branch, 1, &subfield, NULL);
		avro_value_set_branch(&subfield, 1, &subbranch);
		avro_value_set_long(&subbranch, bam1->core.qual);

		// alignment . 2 cigar
		//avro_value_get_by_name(&branch, "cigar", &subfield, NULL);
		avro_value_get_by_index(&branch, 2, &subfield, NULL);
		uint32_t op, *cigar = bam1_cigar(bam1);
		int num_ops = bam1->core.n_cigar;
		for (int i = 0; i < num_ops; i++) {
			avro_value_append(&subfield, &element, NULL);

			// alignment . cigar . 0 operation
			//avro_value_get_by_name(&element, "operation", &subsubfield, NULL);
			avro_value_get_by_index(&element, 0, &subsubfield, NULL);
			op = cigar[i];
			switch (op & BAM_CIGAR_MASK) {
			case BAM_CMATCH:  // M: match or mismatch
				avro_value_set_enum(&subsubfield, 0); // ALIGNMENT_MATCH
				break;
			case BAM_CINS:  // I: insertion to the reference
				avro_value_set_enum(&subsubfield, 1); // INSERT
				break;
			case BAM_CDEL:  // D: deletion from the reference
				avro_value_set_enum(&subsubfield, 2); // DELETE
				break;
			case BAM_CREF_SKIP: //N: skip on the reference
				avro_value_set_enum(&subsubfield, 3); // SKIP
				break;
			case BAM_CSOFT_CLIP: //S: clip on the read with clipped sequence
				avro_value_set_enum(&subsubfield, 4); // CLIP_SOFT
				break;
			case BAM_CHARD_CLIP: //H: clip on the read with clipped sequence trimmed off
				avro_value_set_enum(&subsubfield, 5); // CLIP_HARD
				break;
			case BAM_CPAD:  //P: padding
				avro_value_set_enum(&subsubfield, 6); // PAD
				break;
			case BAM_CEQUAL:  //=: match
				avro_value_set_enum(&subsubfield, 7); // SEQUENCE_MATCH
				break;
			case BAM_CDIFF:  //X: mismatch
				avro_value_set_enum(&subsubfield,  8); // SEQUENCE_MISMATCH
				break;
			}

			// alignment . cigar . 1 operationLength
			//avro_value_get_by_name(&element, "operationLength", &subsubfield, NULL);
			avro_value_get_by_index(&element, 1, &subsubfield, NULL);
			avro_value_set_long(&subsubfield, op >> BAM_CIGAR_SHIFT);

			// alignment . cigar . 2 referenceSequence
			//avro_value_get_by_name(&element, "referenceSequence", &subsubfield, NULL);
			avro_value_get_by_index(&element, 2, &subsubfield, NULL);
			avro_value_set_branch(&subsubfield, 0, &subbranch);
			avro_value_set_null(&subbranch);
		}
	}

	// 10 secondaryAlignment
	aux = ((bam1->core.flag & BAM_FSECONDARY) == BAM_FSECONDARY);
	//avro_value_get_by_name(&read_alignment, "secondaryAlignment", &field, NULL);
	avro_value_get_by_index(&read_alignment, 10, &field, NULL);
	avro_value_set_branch(&field, 0, &branch);
	avro_value_set_boolean(&branch, aux);

	// 11 supplementaryAlignment
	aux = ((bam1->core.flag & BAM_FSECONDARY) == BAM_FSECONDARY);
	//avro_value_get_by_name(&read_alignment, "supplementaryAlignment", &field, NULL);
	avro_value_get_by_index(&read_alignment, 11, &field, NULL);
	avro_value_set_branch(&field, 0, &branch);
	avro_value_set_boolean(&branch, aux);

	// 12 alignedSequence and 13 alignedQuality
	int qlen = bam1->core.l_qseq;
	char buf[qlen + 1];
	const uint8_t *seq = bam_get_seq(bam1);
	const uint8_t *qual = bam1_qual(bam1);
	buf[qlen] = 0;

	//avro_value_get_by_name(&read_alignment, "alignedQuality", &field, NULL);
	avro_value_get_by_index(&read_alignment, 13, &field, NULL);
	for (int i = 0; i < qlen; i++) {
		// sequence
		buf[i] = bam_nt16_rev_table[bam1_seqi(seq, i)];

		// quality
		avro_value_append(&field, &element, NULL);
		avro_value_set_int(&element, qual[i] + 33);
	}
	//avro_value_get_by_name(&read_alignment, "alignedSequence", &field, NULL);
	avro_value_get_by_index(&read_alignment, 12, &field, NULL);
	avro_value_set_branch(&field, 1, &branch);
	avro_value_set_string(&branch, buf);

	// 14 nextMatePosition
	//avro_value_get_by_name(&read_alignment, "nextMatePosition", &field, NULL);
	avro_value_get_by_index(&read_alignment, 14, &field, NULL);
	if (bam1->core.flag & BAM_FPAIRED) {
		avro_value_set_branch(&field, 1, &branch);

		// nextMatePosition . 0 referenceName
		//avro_value_get_by_name(&branch, "referenceName", &subfield, NULL);
		avro_value_get_by_index(&branch, 0, &subfield, NULL);
		avro_value_set_branch(&subfield, 1, &subbranch);
		avro_value_set_string(&subbranch, bam_header->target_name[bam1->core.mtid]);

		// nextMatePosition . 1 sequenceId
		//avro_value_get_by_name(&branch, "sequenceId", &subfield, NULL);
		avro_value_get_by_index(&branch, 1, &subfield, NULL);
		avro_value_set_branch(&subfield, 0, &subbranch);
		avro_value_set_null(&subbranch);

		// nextMatePosition . 2 position
		//avro_value_get_by_name(&branch, "position", &subfield, NULL);
		avro_value_get_by_index(&branch, 2, &subfield, NULL);
		avro_value_set_long(&subfield, bam1->core.mpos);

		// nextMatePosition . 3 strand
		//avro_value_get_by_name(&branch, "strand", &subfield, NULL);
		avro_value_get_by_index(&branch, 3, &subfield, NULL);
		if (bam1->core.flag & BAM_FMREVERSE) {
			avro_value_set_enum(&subfield, 1);
		} else {
			avro_value_set_enum(&subfield, 0);
		}
	} else {
		avro_value_set_branch(&field, 0, &branch);
		avro_value_set_null(&branch);
	}

	// 15 info
	//avro_value_get_by_name(&read_alignment, "info", &field, NULL);
	avro_value_get_by_index(&read_alignment, 15, &field, NULL);

	uint8_t type, key[3];
	uint8_t *s = bam_get_aux(bam1);
	while (s + 4 < bam1->data + bam1->l_data) {
		key[0] = s[0]; key[1] = s[1], key[2] = 0;
		s += 2; type = *s++;
		//    printf("\t%s:", key);
		//kputc('\t', str); kputsn((char*)key, 2, str); kputc(':', str);
		avro_value_add(&field, key, &element, NULL, NULL);

		if (type == 'A') {
			//      printf("A:"); //kputsn("A:", 2, str);
			//      printf("%c", *s); //kputc(*s, str);
			sprintf(buffer, "%c", *s);

			avro_value_append(&element, &subelement, NULL);
			avro_value_set_string(&subelement, "A");

			avro_value_append(&element, &subelement, NULL);
			avro_value_set_string(&subelement, buffer);

			++s;
		} else if (type == 'C') {
			//    printf("i:"); //kputsn("i:", 2, str);
			//      printf("%u", *s); //kputw(*s, str);
			sprintf(buffer, "%u", *s);

			avro_value_append(&element, &subelement, NULL);
			avro_value_set_string(&subelement, "i");

			avro_value_append(&element, &subelement, NULL);
			avro_value_set_string(&subelement, buffer);

			++s;
		} else if (type == 'c') {
			//      printf("i:"); //kputsn("i:", 2, str);
			//      printf("%i", *s); //kputw(*(int8_t*)s, str);
			sprintf(buffer, "%i", *s);

			avro_value_append(&element, &subelement, NULL);
			avro_value_set_string(&subelement, "i");

			avro_value_append(&element, &subelement, NULL);
			avro_value_set_string(&subelement, buffer);

			++s;
		} else if (type == 'S') {
			if (s+2 <= bam1->data + bam1->l_data) {
				//	printf("i:"); //kputsn("i:", 2, str);
				//	printf("%u", *(uint16_t*)s); //kputw(*(uint16_t*)s, str);
				sprintf(buffer, "%u", *(uint16_t*)s);

				avro_value_append(&element, &subelement, NULL);
				avro_value_set_string(&subelement, "i");

				avro_value_append(&element, &subelement, NULL);
				avro_value_set_string(&subelement, buffer);

				s += 2;
			} else {
				fprintf(stderr, "Unable to parse optional fields %s:%c.\n", key, type);
				exit(EXIT_FAILURE);
			}
		} else if (type == 's') {
			if (s+2 <= bam1->data + bam1->l_data) {
				//	printf("i:"); //kputsn("i:", 2, str);
				//	printf("%i", *(int16_t*)s); //kputw(*(int16_t*)s, str);
				sprintf(buffer, "%i", *(int16_t*)s);

				avro_value_append(&element, &subelement, NULL);
				avro_value_set_string(&subelement, "i");

				avro_value_append(&element, &subelement, NULL);
				avro_value_set_string(&subelement, buffer);

				s += 2;
			} else {
				fprintf(stderr, "Unable to parse optional fields %s:%c.\n", key, type);
				exit(EXIT_FAILURE);
			}
		} else if (type == 'I') {
			if (s+4 <= bam1->data + bam1->l_data) {
				//	printf("i:"); //kputsn("i:", 2, str);
				//	printf("%u", *(uint32_t*)s); //kputuw(*(uint32_t*)s, str);
				sprintf(buffer, "%u", *(uint32_t*)s);

				avro_value_append(&element, &subelement, NULL);
				avro_value_set_string(&subelement, "i");

				avro_value_append(&element, &subelement, NULL);
				avro_value_set_string(&subelement, buffer);

				s += 4;
			} else {
				fprintf(stderr, "Unable to parse optional fields %s:%c.\n", key, type);
				exit(EXIT_FAILURE);
			}
		} else if (type == 'i') {
			if (s+4 <= bam1->data + bam1->l_data) {
				//	printf("i:"); //kputsn("i:", 2, str);
				//	printf("%i", *(int32_t*)s); //kputw(*(int32_t*)s, str);
				sprintf(buffer, "%i", *(int32_t*)s);

				avro_value_append(&element, &subelement, NULL);
				avro_value_set_string(&subelement, "i");

				avro_value_append(&element, &subelement, NULL);
				avro_value_set_string(&subelement, buffer);

				s += 4;
			} else {
				fprintf(stderr, "Unable to parse optional fields %s:%c.\n", key, type);
				exit(EXIT_FAILURE);
			}
		} else if (type == 'f') {
			if (s+4 <= bam1->data + bam1->l_data) {
				//	printf("f:%g", *(float*)s); //ksprintf(str, "f:%g", *(float*)s);
				sprintf(buffer, "%g", *(float*)s);

				avro_value_append(&element, &subelement, NULL);
				avro_value_set_string(&subelement, "f");

				avro_value_append(&element, &subelement, NULL);
				avro_value_set_string(&subelement, buffer);

				s += 4;
			} else {
				fprintf(stderr, "Unable to parse optional fields %s:%c.\n", key, type);
				exit(EXIT_FAILURE);
			}
		} else if (type == 'd') {
			if (s+8 <= bam1->data + bam1->l_data) {
				//	printf("d:%g", *(double*)s); //ksprintf(str, "d:%g", *(double*)s);
				sprintf(buffer, "%g", *(double*)s);

				avro_value_append(&element, &subelement, NULL);
				avro_value_set_string(&subelement, "g");

				avro_value_append(&element, &subelement, NULL);
				avro_value_set_string(&subelement, buffer);

				s += 8;
			} else {
				fprintf(stderr, "Unable to parse optional fields %s:%c.\n", key, type);
				exit(EXIT_FAILURE);
			}
		} else if (type == 'Z') {
			//      printf("%c", type); //kputc(type, str);
			//      printf(":"); //kputc(':', str);
			int count = 0;
			while (s < bam1->data + bam1->l_data && *s) {
				buffer[count++] = *s++;
				if (count >= MAX_BUFFER_SIZE) {
					fprintf(stderr, "Overflow parsing optional fields %s:%c (read %s).\n", key, type, bam1_qname(bam1));
					exit(EXIT_FAILURE);
				}
				//	printf("%c", *s++); //kputc(*s++, str);

			}

			if (s >= bam1->data + bam1->l_data) {
				fprintf(stderr, "Unable to parse optional fields %s:%c.\n", key, type);
				exit(EXIT_FAILURE);
			}

			avro_value_append(&element, &subelement, NULL);
			avro_value_set_string(&subelement, "Z");

			avro_value_append(&element, &subelement, NULL);
			avro_value_set_string(&subelement, buffer);

			++s;
		} else if (type == 'H') {
			//      printf("%c", type); //kputc(type, str);
			//      printf(":"); //kputc(':', str);
			int count = 0;
			while (s < bam1->data + bam1->l_data && *s) {
				buffer[count++] = *s++;
				if (count >= MAX_BUFFER_SIZE) {
					fprintf(stderr, "Overflow parsing optional fields %s:%c (read %s).\n", key, type, bam1_qname(bam1));
					exit(EXIT_FAILURE);
				}
				//	printf("%c", *s++); //kputc(*s++, str);
			}

			if (s >= bam1->data + bam1->l_data) {
				fprintf(stderr, "Unable to parse optional fields %s:%c.\n", key, type);
				exit(EXIT_FAILURE);
			}

			avro_value_append(&element, &subelement, NULL);
			avro_value_set_string(&subelement, "H");

			avro_value_append(&element, &subelement, NULL);
			avro_value_set_string(&subelement, buffer);

			++s;
		} else if (type == 'B') {
			fprintf(stderr, "Unable to parse optional fields %s:%c. Not implemented yet.\n", key, type);
			exit(EXIT_FAILURE);

			uint8_t sub_type = *(s++);
			int32_t n;
			memcpy(&n, s, 4);
			s += 4; // no point to the start of the array
			if (s + n >= bam1->data + bam1->l_data) {
				fprintf(stderr, "Unable to parse optional fields %s:%c.\n", key, type);
				exit(EXIT_FAILURE);
			}
			//      printf("B:"); //kputsn("B:", 2, str);
			//      printf("%c", sub_type); // kputc(sub_type, str); // write the typing
			for (int i = 0; i < n; ++i) { // FIXME: for better performance, put the loop after "if"
				//	printf(","); //kputc(',', str);
				if ('c' == sub_type)      {
					//	  printf("%i", *(int8_t*)s); //kputw(*(int8_t*)s, str);
					++s;
				} else if ('C' == sub_type) {
					//	  printf("%u", *(uint8_t*)s); //kputw(*(uint8_t*)s, str);
					++s;
				} else if ('s' == sub_type) {
					//	  printf("%i", *(int16_t*)s); //kputw(*(int16_t*)s, str);
					s += 2;
				} else if ('S' == sub_type) {
					//	  printf("%u", *(uint16_t*)s); //kputw(*(uint16_t*)s, str);
					s += 2;
				} else if ('i' == sub_type) {
					//	  printf("%i", *(int32_t*)s); //kputw(*(int32_t*)s, str);
					s += 4;
				} else if ('I' == sub_type) {
					//	  printf("%u", *(uint32_t*)s); //kputuw(*(uint32_t*)s, str);
					s += 4;
				} else if ('f' == sub_type) {
					//	  printf("%g", *(float*)s); //ksprintf(str, "%g", *(float*)s);
					s += 4;
				}
			}
		}
	}

	// write ReadAlignment
	if (avro_file_writer_append_value(db, &read_alignment)) {
		fprintf(stderr, "Unable to write ReadAlignment datum to memory buffer.\nError message: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}
}

//--------------------------------------------------------------------//
// add read alignment
//
// old Avro API (avro_datum_xxx)
//--------------------------------------------------------------------//

// add read alignment
void add_read_alignment(avro_file_writer_t db, const bam1_t *bam1, const bam_header_t *bam_header) {
	int rval, aux;

	avro_datum_t read_alignment = avro_record(read_alignment_schema);

	// id
	avro_datum_t id = avro_string(bam1_qname(bam1));
	avro_datum_t id_datum = avro_union(union_schema_ns, 1, id);
	if (avro_record_set(read_alignment, "id", id_datum)) {
		fprintf(stderr, "Unable to set id.\nError message: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}
	avro_datum_decref(id);
	avro_datum_decref(id_datum);

	// readGroupId
	avro_datum_t read_group_id_datum = avro_string("read-group-id-value");
	if (avro_record_set(read_alignment, "readGroupId", read_group_id_datum)) {
		fprintf(stderr, "Unable to set readGroupId.\nError message: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}
	avro_datum_decref(read_group_id_datum);

	// fragmentName
	avro_datum_t fragment_name_datum = avro_string(bam_header->target_name[bam1->core.tid]);
	if (avro_record_set(read_alignment, "fragmentName", fragment_name_datum)) {
		fprintf(stderr, "Unable to set fragmentName.\nError message: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}
	avro_datum_decref(fragment_name_datum);

	// properPlacement
	aux = ((bam1->core.flag & BAM_FPROPER_PAIR) && (bam1->core.flag & BAM_FPAIRED));
	avro_datum_t proper_placement = avro_boolean(aux);
	avro_datum_t proper_placement_datum = avro_union(union_schema_bn, 0, proper_placement);
	if (avro_record_set(read_alignment, "properPlacement", proper_placement_datum)) {
		fprintf(stderr, "Unable to set properPlacement.\nError message: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}
	avro_datum_decref(proper_placement);
	avro_datum_decref(proper_placement_datum);

	// duplicateFragment
	aux = ((bam1->core.flag & BAM_FDUP) == BAM_FDUP);
	avro_datum_t duplicate_fragment = avro_boolean(aux);
	avro_datum_t duplicate_fragment_datum = avro_union(union_schema_bn, 0, duplicate_fragment);
	if (avro_record_set(read_alignment, "duplicateFragment", duplicate_fragment_datum)) {
		fprintf(stderr, "Unable to set duplicateFragment.\nError message: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}
	avro_datum_decref(duplicate_fragment);
	avro_datum_decref(duplicate_fragment_datum);

	// numberReads
	int num_reads = (bam1->core.flag & BAM_FPROPER_PAIR) ? 2 : 1;
	avro_datum_t number_reads = avro_int32(num_reads);
	avro_datum_t number_reads_datum = avro_union(union_schema_ni, 1, number_reads);
	if (avro_record_set(read_alignment, "numberReads", number_reads_datum)) {
		fprintf(stderr, "Unable to set numberReads.\nError message: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}
	avro_datum_decref(number_reads);
	avro_datum_decref(number_reads_datum);

	// fragmentLength
	aux = (bam1->core.flag & BAM_FPAIRED) ? bam1->core.isize : 0;
	avro_datum_t fragment_length = avro_int32(aux);
	avro_datum_t fragment_length_datum = avro_union(union_schema_ni, 1, fragment_length);
	if (avro_record_set(read_alignment, "fragmentLength", fragment_length_datum)) {
		fprintf(stderr, "Unable to set fragmentLength.\nError message: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}
	avro_datum_decref(fragment_length);
	avro_datum_decref(fragment_length_datum);

	// readNumber
	aux = ((bam1->core.flag & BAM_FPAIRED) && (bam1->core.flag & BAM_FREAD2)) ? num_reads - 1 : 0;
	avro_datum_t read_number = avro_int32(aux);
	avro_datum_t read_number_datum = avro_union(union_schema_ni, 1, read_number);
	if (avro_record_set(read_alignment, "readNumber", read_number_datum)) {
		fprintf(stderr, "Unable to set readNumber.\nError message: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}
	avro_datum_decref(read_number);
	avro_datum_decref(read_number_datum);

	// failedVendorQualityChecks
	aux = ((bam1->core.flag & BAM_FQCFAIL) == BAM_FQCFAIL);
	avro_datum_t failed_vendor = avro_boolean(aux);
	avro_datum_t failed_vendor_datum = avro_union(union_schema_bn, 0, failed_vendor);
	if (avro_record_set(read_alignment, "failedVendorQualityChecks", failed_vendor_datum)) {
		fprintf(stderr, "Unable to set failedVendorQualityChecks.\nError message: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}
	avro_datum_decref(failed_vendor);
	avro_datum_decref(failed_vendor_datum);

	// alignment
	if (bam1->core.flag & BAM_FUNMAP) {
		// unmapped
		avro_datum_t alignment = avro_null();
		avro_datum_t alignment_datum  = avro_union(union_schema_na, 0, alignment);
		if (avro_record_set(read_alignment, "alignment", alignment_datum)) {
			fprintf(stderr, "Unable to set alignment (null).\nError message: %s\n", avro_strerror());
			exit(EXIT_FAILURE);
		}
		avro_datum_decref(alignment);
		avro_datum_decref(alignment_datum);
	} else {
		// linear alignment
		avro_datum_t linear_alignment_datum = avro_record(linear_alignment_schema);

		// position
		avro_datum_t position_datum = avro_record(position_schema);

		avro_datum_t ref_name_pos = avro_string(bam_header->target_name[bam1->core.tid]);
		avro_datum_t ref_name_pos_datum = avro_union(union_schema_ns, 1, ref_name_pos);
		if (avro_record_set(position_datum, "referenceName", ref_name_pos_datum)) {
			fprintf(stderr, "Unable to set referenceName in LinearAlignment.Position.\nError message: %s\n", avro_strerror());
			exit(EXIT_FAILURE);
		}
		avro_datum_decref(ref_name_pos);
		avro_datum_decref(ref_name_pos_datum);

		avro_datum_t seq_id_pos = avro_null();
		avro_datum_t seq_id_pos_datum = avro_union(union_schema_ns, 0, seq_id_pos);
		if (avro_record_set(position_datum, "sequenceId", seq_id_pos_datum)) {
			fprintf(stderr, "Unable to set sequenceId in LinearAlignment.Position.\nError message: %s\n", avro_strerror());
			exit(EXIT_FAILURE);
		}
		avro_datum_decref(seq_id_pos);
		avro_datum_decref(seq_id_pos_datum);

		avro_datum_t pos_datum = avro_int64(bam1->core.pos);
		if (avro_record_set(position_datum, "position", pos_datum)) {
			fprintf(stderr, "Unable to set position in LinearAlignment.Position.\nError message: %s\n", avro_strerror());
			exit(EXIT_FAILURE);
		}
		avro_datum_decref(pos_datum);

		avro_datum_t strand_datum;
		avro_schema_t strand_enum = avro_schema_enum("Strand");
		if (bam1->core.flag & BAM_FREVERSE) {
			strand_datum = avro_enum(strand_enum, 1);
		} else {
			strand_datum = avro_enum(strand_enum, 0);
		}
		if (avro_record_set(position_datum, "strand", strand_datum)) {
			fprintf(stderr, "Unable to set strand in LinearAlignment.Position.\nError message: %s\n", avro_strerror());
			exit(EXIT_FAILURE);
		}
		avro_datum_decref(strand_datum);
		avro_schema_decref(strand_enum);

		if (avro_record_set(linear_alignment_datum, "position", position_datum)) {
			fprintf(stderr, "Unable to set strand in LinearAlignment.Position.\nError message: %s\n", avro_strerror());
			exit(EXIT_FAILURE);
		}
		avro_datum_decref(position_datum);

		// mapping quality
		avro_datum_t mapping_quality = avro_int32(bam1->core.qual);
		avro_datum_t mapping_quality_datum = avro_union(union_schema_ni, 1, mapping_quality);
		if (avro_record_set(linear_alignment_datum, "mappingQuality", mapping_quality_datum)) {
			fprintf(stderr, "Unable to set mappingQuality in LinearAlignment.\nError message: %s\n", avro_strerror());
			exit(EXIT_FAILURE);
		}
		avro_datum_decref(mapping_quality);
		avro_datum_decref(mapping_quality_datum);

		// cigar
		avro_schema_t cigar_unit_array_schema = avro_schema_array(cigar_unit_schema);
		avro_datum_t cigar_datum = avro_array(cigar_unit_array_schema);

		uint32_t op, *cigar = bam1_cigar(bam1);
		int num_ops = bam1->core.n_cigar;
		for (int i = 0; i < num_ops; i++) {
			avro_datum_t cigar_unit_datum = avro_record(cigar_unit_schema);

			avro_schema_t cigar_operation_enum = avro_schema_enum("CigarOperation");

			avro_datum_t cigar_operation_datum;
			op = cigar[i];
			switch (op & BAM_CIGAR_MASK) {
			case BAM_CMATCH:  // M: match or mismatch
				cigar_operation_datum = avro_enum(cigar_operation_enum, 0); // ALIGNMENT_MATCH
				break;
			case BAM_CINS:  // I: insertion to the reference
				cigar_operation_datum = avro_enum(cigar_operation_enum, 1); // INSERT
				break;
			case BAM_CDEL:  // D: deletion from the reference
				cigar_operation_datum = avro_enum(cigar_operation_enum, 2); // DELETE
				break;
			case BAM_CREF_SKIP: //N: skip on the reference
				cigar_operation_datum = avro_enum(cigar_operation_enum, 3); // SKIP
				break;
			case BAM_CSOFT_CLIP: //S: clip on the read with clipped sequence
				cigar_operation_datum = avro_enum(cigar_operation_enum, 4); // CLIP_SOFT
				break;
			case BAM_CHARD_CLIP: //H: clip on the read with clipped sequence trimmed off
				cigar_operation_datum = avro_enum(cigar_operation_enum, 5); // CLIP_HARD
				break;
			case BAM_CPAD:  //P: padding
				cigar_operation_datum = avro_enum(cigar_operation_enum, 6); // PAD
				break;
			case BAM_CEQUAL:  //=: match
				cigar_operation_datum = avro_enum(cigar_operation_enum, 7); // SEQUENCE_MATCH
				break;
			case BAM_CDIFF:  //X: mismatch
				cigar_operation_datum = avro_enum(cigar_operation_enum, 8); // SEQUENCE_MISMATCH
				break;
			}
			if (avro_record_set(cigar_unit_datum, "operation", cigar_operation_datum)) {
				fprintf(stderr, "Unable to set operation in CigarUnit.\nError message: %s\n", avro_strerror());
				exit(EXIT_FAILURE);
			}
			avro_datum_decref(cigar_operation_datum);
			avro_schema_decref(cigar_operation_enum);

			avro_datum_t op_length_datum = avro_int64(op >> BAM_CIGAR_SHIFT);
			if (avro_record_set(cigar_unit_datum, "operationLength", op_length_datum)) {
				fprintf(stderr, "Unable to set operationLength in CigarUnit.\nError message: %s\n", avro_strerror());
				exit(EXIT_FAILURE);
			}
			avro_datum_decref(op_length_datum);

			avro_datum_t ref_seq = avro_null();
			avro_datum_t ref_seq_datum = avro_union(union_schema_ns, 0, ref_seq);
			if (avro_record_set(cigar_unit_datum, "referenceSequence", ref_seq_datum)) {
				fprintf(stderr, "Unable to set referenceSequence in CigarUnit.\nError message: %s\n", avro_strerror());
				exit(EXIT_FAILURE);
			}
			avro_datum_decref(ref_seq);
			avro_datum_decref(ref_seq_datum);

			rval = avro_array_append_datum(cigar_datum, cigar_unit_datum);
			avro_datum_decref(cigar_unit_datum);
		}

		if (avro_record_set(linear_alignment_datum, "cigar", cigar_datum)) {
			fprintf(stderr, "Unable to set cigar in LinearAlignment.\nError message: %s\n", avro_strerror());
			exit(EXIT_FAILURE);
		}
		avro_datum_decref(cigar_datum);
		avro_schema_decref(cigar_unit_array_schema);

		avro_datum_t alignment_datum = avro_union(union_schema_na, 1, linear_alignment_datum);
		if (avro_record_set(read_alignment, "alignment", alignment_datum)) {
			fprintf(stderr, "Unable to set alignment (null).\nError message: %s\n", avro_strerror());
			exit(EXIT_FAILURE);
		}
		avro_datum_decref(linear_alignment_datum);
		avro_datum_decref(alignment_datum);
	}

	// secondaryAlignment
	aux = ((bam1->core.flag & BAM_FSECONDARY) == BAM_FSECONDARY);
	avro_datum_t secondary_alignment = avro_boolean(aux);
	avro_datum_t secondary_alignment_datum = avro_union(union_schema_bn, 0, secondary_alignment);
	if (avro_record_set(read_alignment, "secondaryAlignment", secondary_alignment_datum)) {
		fprintf(stderr, "Unable to set secondaryAlignment.\nError message: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}
	avro_datum_decref(secondary_alignment);
	avro_datum_decref(secondary_alignment_datum);

	// supplementaryAlignment
	aux = ((bam1->core.flag & BAM_FSECONDARY) == BAM_FSECONDARY);
	avro_datum_t supplementary_alignment = avro_boolean(aux);
	avro_datum_t supplementary_alignment_datum = avro_union(union_schema_bn, 0, supplementary_alignment);
	if (avro_record_set(read_alignment, "supplementaryAlignment", supplementary_alignment_datum)) {
		fprintf(stderr, "Unable to set supplementaryAlignment.\nError message: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}
	avro_datum_decref(supplementary_alignment);
	avro_datum_decref(supplementary_alignment_datum);

	// alignedSequence
	int qlen = bam1->core.l_qseq;
	char buf[qlen + 1];
	char *seq = bam1_seq(bam1);
	buf[qlen] = 0;  for (int i = 0; i < qlen; i++) {
		buf[i] = bam_nt16_rev_table[bam1_seqi(seq, i)];
	}
	avro_datum_t aligned_sequence = avro_string(buf);
	avro_datum_t aligned_sequence_datum = avro_union(union_schema_ns, 1, aligned_sequence);
	if (avro_record_set(read_alignment, "alignedSequence", aligned_sequence_datum)) {
		fprintf(stderr, "Unable to set alignedSequence.\nError message: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}
	avro_datum_decref(aligned_sequence);
	avro_datum_decref(aligned_sequence_datum);

	// alignedQuality
	seq = bam1_qual(bam1);
	avro_schema_t int_array_schema = avro_schema_array(avro_schema_int());
	avro_datum_t aligned_quality_datum = avro_array(int_array_schema);
	for (int i = 0; i < qlen; i++) {
		avro_datum_t i32_datum = avro_int32(33 + seq[i]);
		rval = avro_array_append_datum(aligned_quality_datum, i32_datum);
		avro_datum_decref(i32_datum);
		if (rval) {
			fprintf(stderr, "Unable to set item %i (%i) in alignedQuality.\nError message: %s\n", i, 33 + seq[i], avro_strerror());
			exit(EXIT_FAILURE);
		}
	}
	if (avro_record_set(read_alignment, "alignedQuality", aligned_quality_datum)) {
		fprintf(stderr, "Unable to set alignedQuality.\nError message: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}
	avro_datum_decref(aligned_quality_datum);
	avro_schema_decref(int_array_schema);

	// nextMatePosition
	if (bam1->core.flag & BAM_FPAIRED) {
		avro_datum_t next_mate_position = avro_record(position_schema);

		avro_datum_t ref_name_pos = avro_string(bam_header->target_name[bam1->core.mtid]);
		avro_datum_t ref_name_pos_datum = avro_union(union_schema_ns, 1, ref_name_pos);
		if (avro_record_set(next_mate_position, "referenceName", ref_name_pos_datum)) {
			fprintf(stderr, "Unable to set referenceName in nextMatePosition.\nError message: %s\n", avro_strerror());
			exit(EXIT_FAILURE);
		}
		avro_datum_decref(ref_name_pos);
		avro_datum_decref(ref_name_pos_datum);

		avro_datum_t seq_id_pos = avro_null();
		avro_datum_t seq_id_pos_datum = avro_union(union_schema_ns, 0, seq_id_pos);
		if (avro_record_set(next_mate_position, "sequenceId", seq_id_pos_datum)) {
			fprintf(stderr, "Unable to set sequenceId in nextMatePosition.\nError message: %s\n", avro_strerror());
			exit(EXIT_FAILURE);
		}
		avro_datum_decref(seq_id_pos);
		avro_datum_decref(seq_id_pos_datum);

		avro_datum_t pos_datum = avro_int64(bam1->core.mpos);
		if (avro_record_set(next_mate_position, "position", pos_datum)) {
			fprintf(stderr, "Unable to set position in nextMatePosition.\nError message: %s\n", avro_strerror());
			exit(EXIT_FAILURE);
		}
		avro_datum_decref(pos_datum);

		avro_datum_t strand_datum;
		avro_schema_t strand_enum = avro_schema_enum("Strand");
		if (bam1->core.flag & BAM_FMREVERSE) {
			strand_datum = avro_enum(strand_enum, 1);
		} else {
			strand_datum = avro_enum(strand_enum, 0);
		}
		if (avro_record_set(next_mate_position, "strand", strand_datum)) {
			fprintf(stderr, "Unable to set strand in nextMatePosition.\nError message: %s\n", avro_strerror());
			exit(EXIT_FAILURE);
		}
		avro_datum_decref(strand_datum);
		avro_schema_decref(strand_enum);

		avro_datum_t next_mate_pos_datum = avro_union(union_schema_np, 1, next_mate_position);
		if (avro_record_set(read_alignment, "nextMatePosition", next_mate_pos_datum)) {
			fprintf(stderr, "Unable to set nextMatePosition.\nError message: %s\n", avro_strerror());
			exit(EXIT_FAILURE);
		}
		avro_datum_decref(next_mate_position);
		avro_datum_decref(next_mate_pos_datum);
	} else {
		avro_datum_t next_mate_position = avro_null();
		avro_datum_t next_mate_pos_datum = avro_union(union_schema_np, 0, next_mate_position);
		if (avro_record_set(read_alignment, "nextMatePosition", next_mate_pos_datum)) {
			fprintf(stderr, "Unable to set nextMatePosition.\nError message: %s\n", avro_strerror());
			exit(EXIT_FAILURE);
		}
		avro_datum_decref(next_mate_position);
		avro_datum_decref(next_mate_pos_datum);
	}

	// info
	avro_schema_t string_array_schema = avro_schema_array(avro_schema_string());
	avro_schema_t map_schema = avro_schema_map(string_array_schema);
	avro_datum_t info_datum = avro_map(map_schema);

	/*
  avro_datum_t optional_datum = avro_array(string_array_schema);
  avro_datum_t opt_type_datum = avro_string("i");
  avro_datum_t opt_value_datum = avro_string("125");
	 */
	/*
  rval = avro_array_append_datum(optional_datum, opt_type_datum);
  if (rval) {
    fprintf(stderr, "Unable to append array datum.\nError message: %s\n", avro_strerror());
    exit(EXIT_FAILURE);
  }
  rval = avro_array_append_datum(optional_datum, opt_value_datum);
  if (rval) {
    fprintf(stderr, "Unable to append array datum.\nError message: %s\n", avro_strerror());
    exit(EXIT_FAILURE);
  }
	 */
	/*
  avro_map_set(info_datum, "NM", optional_datum);
  avro_datum_decref(optional_datum);
  avro_datum_decref(opt_type_datum);
  avro_datum_decref(opt_value_datum);
	 */
	/*
  char value[1024];
  uint8_t *s = bam_get_aux(bam1);
  while (s + 4 < bam1->data + bam1->l_data) {
    uint8_t type, key[3];
    key[0] = s[0]; key[1] = s[1], key[2] = 0;
    s += 2; type = *s++;
    printf("\t%s:", key);
    //kputc('\t', str); kputsn((char*)key, 2, str); kputc(':', str);

    if (type == 'A') {
      printf("A:"); //kputsn("A:", 2, str);
      printf("%c", *s); //kputc(*s, str);
      sprintf(value, "%c", *s);

      avro_datum_t optional_datum = avro_array(string_array_schema);
      rval = avro_array_append_datum(optional_datum, avro_string("A"));
      rval = avro_array_append_datum(optional_datum, avro_string(value));
      avro_map_set(info_datum, key, optional_datum);

      ++s;
    } else if (type == 'C') {
      printf("i:"); //kputsn("i:", 2, str);
      printf("%u", *s); //kputw(*s, str);
      sprintf(value, "%u", *s);

      avro_datum_t optional_datum = avro_array(string_array_schema);
      rval = avro_array_append_datum(optional_datum, avro_string("i"));
      rval = avro_array_append_datum(optional_datum, avro_string(value));
      avro_map_set(info_datum, key, optional_datum);

      ++s;
    } else if (type == 'c') {
      printf("i:"); //kputsn("i:", 2, str);
      printf("%i", *s); //kputw(*(int8_t*)s, str);
      sprintf(value, "%i", *s);

      avro_datum_t optional_datum = avro_array(string_array_schema);
      rval = avro_array_append_datum(optional_datum, avro_string("i"));
      rval = avro_array_append_datum(optional_datum, avro_string(value));
      avro_map_set(info_datum, key, optional_datum);

      ++s;
    } else if (type == 'S') {
      if (s+2 <= bam1->data + bam1->l_data) {
	printf("i:"); //kputsn("i:", 2, str);
	printf("%u", *(uint16_t*)s); //kputw(*(uint16_t*)s, str);
	sprintf(value, "%u", *(uint16_t*)s);

	avro_datum_t optional_datum = avro_array(string_array_schema);
	rval = avro_array_append_datum(optional_datum, avro_string("i"));
	rval = avro_array_append_datum(optional_datum, avro_string(value));
	avro_map_set(info_datum, key, optional_datum);

	s += 2;
      } else {
	fprintf(stderr, "Unable to parse optional fields %s:%c.\n", key, type);
	exit(EXIT_FAILURE);
      }
    } else if (type == 's') {
      if (s+2 <= bam1->data + bam1->l_data) {
	printf("i:"); //kputsn("i:", 2, str);
	printf("%i", *(int16_t*)s); //kputw(*(int16_t*)s, str);
	sprintf(value, "%i", *(int16_t*)s);

	avro_datum_t optional_datum = avro_array(string_array_schema);
	rval = avro_array_append_datum(optional_datum, avro_string("i"));
	rval = avro_array_append_datum(optional_datum, avro_string(value));
	avro_map_set(info_datum, key, optional_datum);

	s += 2;
      } else {
	fprintf(stderr, "Unable to parse optional fields %s:%c.\n", key, type);
	exit(EXIT_FAILURE);
      }
    } else if (type == 'I') {
      if (s+4 <= bam1->data + bam1->l_data) {
	printf("i:"); //kputsn("i:", 2, str);
	printf("%u", *(uint32_t*)s); //kputuw(*(uint32_t*)s, str);
	sprintf(value, "%u", *(uint32_t*)s);

	avro_datum_t optional_datum = avro_array(string_array_schema);
	rval = avro_array_append_datum(optional_datum, avro_string("i"));
	rval = avro_array_append_datum(optional_datum, avro_string(value));
	avro_map_set(info_datum, key, optional_datum);

	s += 4;
      } else {
	fprintf(stderr, "Unable to parse optional fields %s:%c.\n", key, type);
	exit(EXIT_FAILURE);
      }
    } else if (type == 'i') {
      if (s+4 <= bam1->data + bam1->l_data) {
	printf("i:"); //kputsn("i:", 2, str);
	printf("%i", *(int32_t*)s); //kputw(*(int32_t*)s, str);
	sprintf(value, "%i", *(int32_t*)s);

	avro_datum_t optional_datum = avro_array(string_array_schema);
	rval = avro_array_append_datum(optional_datum, avro_string("i"));
	rval = avro_array_append_datum(optional_datum, avro_string(value));
	avro_map_set(info_datum, key, optional_datum);

	s += 4;
      } else {
	fprintf(stderr, "Unable to parse optional fields %s:%c.\n", key, type);
	exit(EXIT_FAILURE);
      }
    } else if (type == 'f') {
      if (s+4 <= bam1->data + bam1->l_data) {
	printf("f:%g", *(float*)s); //ksprintf(str, "f:%g", *(float*)s);
	sprintf(value, "%g", *(float*)s);

	avro_datum_t optional_datum = avro_array(string_array_schema);
	rval = avro_array_append_datum(optional_datum, avro_string("f"));
	rval = avro_array_append_datum(optional_datum, avro_string(value));
	avro_map_set(info_datum, key, optional_datum);

	s += 4;
      } else {
	fprintf(stderr, "Unable to parse optional fields %s:%c.\n", key, type);
	exit(EXIT_FAILURE);
      }
    } else if (type == 'd') {
      if (s+8 <= bam1->data + bam1->l_data) {
	printf("d:%g", *(double*)s); //ksprintf(str, "d:%g", *(double*)s);
	sprintf(value, "%g", *(double*)s);

	avro_datum_t optional_datum = avro_array(string_array_schema);
	rval = avro_array_append_datum(optional_datum, avro_string("d"));
	rval = avro_array_append_datum(optional_datum, avro_string(value));
	avro_map_set(info_datum, key, optional_datum);

	s += 8;
      } else {
	fprintf(stderr, "Unable to parse optional fields %s:%c.\n", key, type);
	exit(EXIT_FAILURE);
      }
    } else if (type == 'Z' || type == 'H') {
      fprintf(stderr, "Unable to parse optional fields %s:%c. Not implemented yet.\n", key, type);
      exit(EXIT_FAILURE);

      printf("%c", type); //kputc(type, str); 
      printf(":"); //kputc(':', str);
      while (s < bam1->data + bam1->l_data && *s) {
	printf("%c", *s++); //kputc(*s++, str);
      }
      if (s >= bam1->data + bam1->l_data) {
	fprintf(stderr, "Unable to parse optional fields %s:%c.\n", key, type);
	exit(EXIT_FAILURE);
      }
      ++s;
    } else if (type == 'B') {
      fprintf(stderr, "Unable to parse optional fields %s:%c. Not implemented yet.\n", key, type);
      exit(EXIT_FAILURE);

      uint8_t sub_type = *(s++);
      int32_t n;
      memcpy(&n, s, 4);
      s += 4; // no point to the start of the array
      if (s + n >= bam1->data + bam1->l_data) {
	fprintf(stderr, "Unable to parse optional fields %s:%c.\n", key, type);
	exit(EXIT_FAILURE);
      }
      printf("B:"); //kputsn("B:", 2, str); 
      printf("%c", sub_type); // kputc(sub_type, str); // write the typing
      for (int i = 0; i < n; ++i) { // FIXME: for better performance, put the loop after "if"
	printf(","); //kputc(',', str);
	if ('c' == sub_type)      { 
	  printf("%i", *(int8_t*)s); //kputw(*(int8_t*)s, str); 
	  ++s; 
	} else if ('C' == sub_type) { 
	  printf("%u", *(uint8_t*)s); //kputw(*(uint8_t*)s, str); 
	  ++s; 
	} else if ('s' == sub_type) { 
	  printf("%i", *(int16_t*)s); //kputw(*(int16_t*)s, str); 
	  s += 2; 
	} else if ('S' == sub_type) { 
	  printf("%u", *(uint16_t*)s); //kputw(*(uint16_t*)s, str); 
	  s += 2; 
	} else if ('i' == sub_type) { 
	  printf("%i", *(int32_t*)s); //kputw(*(int32_t*)s, str); 
	  s += 4; 
	} else if ('I' == sub_type) { 
	  printf("%u", *(uint32_t*)s); //kputuw(*(uint32_t*)s, str); 
	  s += 4; 
	} else if ('f' == sub_type) { 
	  printf("%g", *(float*)s); //ksprintf(str, "%g", *(float*)s); 
	  s += 4; 
	}
      }
    }
  }
	 */
	if (avro_record_set(read_alignment, "info", info_datum)) {
		fprintf(stderr, "Unable to set info.\nError message: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}
	avro_datum_decref(info_datum);
	avro_schema_decref(string_array_schema);
	avro_schema_decref(map_schema);

	// write ReadAlignment
	if (avro_file_writer_append(db, read_alignment)) {
		fprintf(stderr, "Unable to write ReadAlignment datum to memory buffer.\nError message: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}

	// decrement all our references to prevent memory from leaking
	avro_datum_decref(read_alignment);
}

void bam2ga(const char *bam_filename, const char *avro_filename, const char *codec_name) {
	int rval;
	avro_file_reader_t dbreader;
	avro_file_writer_t db;
	avro_schema_t projection_schema, first_name_schema, phone_schema;
	char number[15] = {0};

	// initialize the schemas structure from JSON
	init_schemas();

	// delete the output filename if it exists
	remove(avro_filename);

	// create a new output filename
	rval = avro_file_writer_create_with_codec
			(avro_filename, read_alignment_schema, &db, codec_name, 0);
	if (rval) {
		fprintf(stderr, "There was an error creating %s\n", avro_filename);
		fprintf(stderr, " error message: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}

	// open BAM
	bamFile bam_fd = bam_open(bam_filename, "r");
	if (bam_fd == NULL) {
		fprintf(stderr, "Error opening BAM file %s\n", bam_filename);
		exit(EXIT_FAILURE);
	}

	// read BAM header
	bam_header_t *bam_header = bam_header_read(bam_fd);

	size_t i = 0;
	bam1_t *bam1 = bam_init1();
	while (bam_read1(bam_fd, bam1) > 0) {
		add_read_alignment2(db, bam1, bam_header);
		//if (i > 100) break;
		if (i > 0 && i % 100000 == 0) printf("%lu reads...\n", i);
		i++;
	}
	printf("Done.\n");

	// free and close BAM
	bam_destroy1(bam1);
	bam_header_destroy(bam_header);
	bam_close(bam_fd);

	// Close the block and open a new one 
	avro_file_writer_flush(db);
	//	add_person(db, "Super", "Man", "123456", 31);

	avro_file_writer_close(db);

	// We don't need this schema anymore 
	free_schemas();
}

/*
int main(int argc, char *argv[]) {
	// check parameters
	if (argc != 4) {
		printf("Usage: %s <bam-file> <avro-file> <codec: null, deflate, snappy, lzma>\n", argv[0]);
		exit(-1);
	}

	bam2ga(argv[1], argv[2], argv[3]);
}
*/

