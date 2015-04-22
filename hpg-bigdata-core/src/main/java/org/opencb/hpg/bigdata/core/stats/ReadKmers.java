package org.opencb.hpg.bigdata.core.stats;

import htsjdk.samtools.fastq.FastqRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.ga4gh.models.Read;
import org.opencb.hpg.bigdata.core.converters.FastqRecord2ReadConverter;

public class ReadKmers {

	public int kvalue;
	public HashMap<String, Integer> kmersMap;

	public ReadKmers() {
		kvalue = 0;
		kmersMap = new HashMap<String, Integer>();		
	}
	
	public void update(ReadKmers kmers) {				
		int value;
		for(String key:kmers.kmersMap.keySet()) {
			value = kmers.kmersMap.get(key);
			if (kmersMap.containsKey(key)) {
				value += kmersMap.get(key);
			}
			kmersMap.put(key, value);
		}
		
		kvalue = kmers.kvalue;
	}

	public void updateByRead(Read read, int kvalue) {
		//updateByFastqRecord(new FastqRecord2ReadConverter().backward(read), kvalue);
		updateBySequence(read.getSequence().toString(), kvalue);
	}

	public void updateBySequence(final String sequence, int k) {
		kvalue = k;
		
		//final String read = fastqRecord.getReadString();
		final int len = sequence.length();
		final int stop = len - kvalue;
		
		String kmer;
		for (int i=0; i < stop; i++) {
			kmer = sequence.substring(i, i + kvalue);
			if (!kmer.contains("N") && !kmer.contains("n")) {
				kmersMap.put(kmer, kmersMap.containsKey(kmer) ? kmersMap.get(kmer) + 1 : 1);
			}
		} // end for
	}
	
	public String toJSON() {
		
		int key;
		TreeMap<Integer, List<String>> sortedMap = new TreeMap<Integer, List<String>>(Collections.reverseOrder());
		for(Map.Entry entry: kmersMap.entrySet()) {
			key = (Integer) entry.getValue();
			if (!sortedMap.containsKey(key)) {
				sortedMap.put(key, new ArrayList<String>());
			}
			sortedMap.get(key).add((String) entry.getKey());
		}
		
		StringBuilder res = new StringBuilder();
		
		res.append("{\"kvalue\": " + kvalue);
		
		int i, size = kmersMap.size();
		res.append(", \"kmers_values\": [");
		i = 0;
		for(Map.Entry entry: sortedMap.entrySet()) {
			for(String value: (List<String>) entry.getValue()) {
				System.out.println(i + " of " + size);
				res.append("[\"" + value + "\", " + (Integer) entry.getKey() + "]");
				if (i >= 19) break;
				if (++i < size) res.append(", ");
			}
			if (i >= 19) break;
		}
		res.append("]}");

		return res.toString();
	}
}
