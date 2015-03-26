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

public class FastqKmers {

	public int kvalue;
	public HashMap<String, Integer> kmersMap;

	public FastqKmers() {
		kvalue = 0;
		kmersMap = new HashMap<String, Integer>();		
	}
	
	public void update(FastqKmers kmers) {				
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
		updateByFastqRecord(new FastqRecord2ReadConverter().backward(read), kvalue);
	}

	public void updateByFastqRecord(FastqRecord fastqRecord, int k) {
		kvalue = k;
		
		final String read = fastqRecord.getReadString();
		final int len = read.length();
		final int stop = len - kvalue;
		
		String kmer;
		for (int i=0; i < stop; i++) {
			kmer = read.substring(i, i + kvalue);
			if (!kmer.contains("N") && !kmer.contains("n")) {
				kmersMap.put(kmer, kmersMap.containsKey(kmer) ? kmersMap.get(kmer) + 1 : 1);
			}
		} // end for
	}
	
	public String toFormat() {
		
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
						
		res.append(kvalue);
		res.append("\n");
		
		res.append(kmersMap.size());
		res.append("\n");
		for(Map.Entry entry: sortedMap.entrySet()) {
			for(String value: (List<String>) entry.getValue()) {
				res.append(value + "\t" + (Integer) entry.getKey());
				res.append("\n");
			}
		}

		return res.toString();
	}
}
