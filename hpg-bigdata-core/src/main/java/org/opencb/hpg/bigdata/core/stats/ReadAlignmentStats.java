package org.opencb.hpg.bigdata.core.stats;

import org.ga4gh.models.ReadAlignment;

public class ReadAlignmentStats {
	
	public ReadStats readStats;
	
	public ReadAlignmentStats () {
		readStats = new ReadStats();
	}
	
	public void update(ReadAlignmentStats value) {
		readStats.update(value.readStats);
	}
	
	public void updateByReadAlignment(ReadAlignment datum) {
		readStats.updateBySequences2(datum.getAlignedSequence().toString(), datum.getAlignedQuality());
	}
	
	public String toJSON() {
		StringBuilder res = new StringBuilder();
		res.append("{");
		res.append("\"read_stats\": " + readStats.toJSON());
		res.append("}");
		return res.toString();
	}

}
