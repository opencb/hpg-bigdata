package org.opencb.hpg.bigdata.core.converters;

import org.opencb.ga4gh.models.Read;

import htsjdk.samtools.fastq.FastqRecord;

public class FastqRecord2ReadConverter implements Converter<FastqRecord, Read> {

	@Override
	public Read forward(FastqRecord obj) {
		return new Read(obj.getReadHeader(), obj.getReadString(), obj.getBaseQualityString());
	}

	@Override
	public FastqRecord backward(Read obj) {
		return new FastqRecord(obj.getId().toString(), obj.getSequence().toString(), "+", obj.getQuality().toString());
	}
	
}