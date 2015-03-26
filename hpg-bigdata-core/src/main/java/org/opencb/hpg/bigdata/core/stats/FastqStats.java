package org.opencb.hpg.bigdata.core.stats;

import htsjdk.samtools.fastq.FastqRecord;

import java.util.HashMap;

import org.ga4gh.models.Read;
import org.opencb.hpg.bigdata.core.converters.FastqRecord2ReadConverter;

public class FastqStats {
	
	public class Info {
		public int numA;
		public int numT;
		public int numG;
		public int numC;
		public int numN;
		public int numQual;
		public int accQual;
		
		public Info() {
			numA = 0;
			numT = 0;
			numG = 0;
			numC = 0;
			numN = 0;
			numQual = 0;
			accQual = 0;
		}
	}
	
	public int numSeqs;
	public int numA;
	public int numT;
	public int numG;
	public int numC;
	public int numN;
	public int minSeqLength;
	public int maxSeqLength;
	public int minSeqQual;
	public int maxSeqQual;
	public int accSeqQual;
	public HashMap<Integer, Integer> lengthMap;
	public HashMap<Integer, Info> infoMap;

	public FastqKmers kmers;
	
	public FastqStats() {
		super();
		
		numSeqs = 0;
		numA = 0;
		numT = 0;
		numG = 0;
		numC = 0;
		numN = 0;
		minSeqLength = Integer.MAX_VALUE;
		maxSeqLength = 0;
		minSeqQual = Integer.MAX_VALUE;
		maxSeqQual = 0;
		accSeqQual = 0;
		lengthMap = new HashMap<Integer, Integer>();
		infoMap = new HashMap<Integer, Info>();
		kmers = new FastqKmers();
	}
	
	public void update(FastqStats stats) {
		numSeqs += stats.numSeqs;
		numA += stats.numA;
		numT += stats.numT;
		numG += stats.numG;
		numC += stats.numC;
		numN += stats.numN;
		
		if (stats.minSeqLength < minSeqLength) {
			minSeqLength = stats.minSeqLength; 
		}
		if (stats.maxSeqLength > maxSeqLength) {
			maxSeqLength = stats.maxSeqLength; 
		}

		if (stats.minSeqQual < minSeqQual) {
			minSeqQual = stats.minSeqQual; 
		}
		if (stats.maxSeqQual > maxSeqQual) {
			maxSeqQual = stats.maxSeqQual; 
		}
		accSeqQual += stats.accSeqQual;
				
		int value;
		for(int key:stats.lengthMap.keySet()) {
			value = stats.lengthMap.get(key);
			if (lengthMap.containsKey(key)) {
				value += lengthMap.get(key);
			}
			lengthMap.put(key, value);
		}

		Info destInfo, info;
		for(int key:stats.infoMap.keySet()) {
			if (infoMap.containsKey(key)) {
				destInfo = infoMap.get(key);
				info = stats.infoMap.get(key);
				destInfo.numA += info.numA;
				destInfo.numT += info.numT;
				destInfo.numG += info.numG;
				destInfo.numC += info.numC;
				destInfo.numN += info.numN;
				destInfo.numQual += info.numQual;
				destInfo.accQual += info.accQual;
			} else {
				destInfo = stats.infoMap.get(key);
			}
			infoMap.put(key, destInfo);
		}
		
		// update kmers, if necessary
		if (stats.kmers.kvalue > 0) {
			kmers.update(stats.kmers);
		}
	}

	public void updateByRead(Read read) {
		updateByFastqRecord(new FastqRecord2ReadConverter().backward(read));
	}

	public void updateByFastqRecord(FastqRecord fastqRecord) {
		numSeqs++;
		Info info = null;
		final String read = fastqRecord.getReadString();
		final String quality = fastqRecord.getBaseQualityString();
		final int len = read.length();
		
		int accQual = 0;
		int qual = 0;
		
		// read length
		lengthMap.put(len, lengthMap.containsKey(len) ? lengthMap.get(len) + 1: 1);
		if (len < minSeqLength) {
			minSeqLength = len;
		}
		if (len > maxSeqLength) {
			maxSeqLength = len;
		}
		
		for (int i=0; i < len; i++) {
			// info management
			if (infoMap.containsKey(i)) {
				info = infoMap.get(i);
			} else {
				info = new Info();
				infoMap.put(i, info);
			}
			
			// quality
			qual = (int) quality.charAt(i);
			accQual += qual;
			info.numQual++;
			info.accQual += qual;

			// nucleotide content
			switch (read.charAt(i)) {
			case 'A':
			case 'a': {
				numA++;
				info.numA++;
				break;
			}
			case 'T':
			case 't': {
				numT++;
				info.numT++;
				break;
			}
			case 'G':
			case 'g': {
				numG++;
				info.numG++;
				break;
			}
			case 'C':
			case 'c': {
				numC++;
				info.numC++;
				break;
			}
			default: {
				numN++;
				info.numN++;
				break;
			}
			} // end switch
		} // end for
		
		// read quality
		accSeqQual += accQual;
		if (accQual < minSeqQual) {
			minSeqQual = accQual;
		}
		if (accQual > maxSeqQual) {
			maxSeqQual = accQual;
		}
		
		// update kmers, if necessary
		if (kmers.kvalue > 0) {
			kmers.updateByFastqRecord(fastqRecord, kmers.kvalue);
		}
	}
	
	public String toFormat() {
		StringBuilder res = new StringBuilder();
		
		res.append(numSeqs);
		res.append("\n");
		res.append(numA);
		res.append("\n");
		res.append(numT);
		res.append("\n");
		res.append(numG);
		res.append("\n");
		res.append(numC);
		res.append("\n");
		res.append(numN);
		res.append("\n");
		
		res.append(minSeqLength);
		res.append("\n");
		int mean = (numA + numT + numG + numC + numN) / numSeqs;
		res.append(mean);
		res.append("\n");
		res.append(maxSeqLength);
		res.append("\n");

		res.append(minSeqQual);
		res.append("\n");
		res.append(accSeqQual / numSeqs);
		res.append("\n");
		res.append(maxSeqQual);
		res.append("\n");
				
		res.append(lengthMap.size());
		res.append("\n");
		for(int key:lengthMap.keySet()) {
			res.append(key + "\t" + lengthMap.get(key));
			res.append("\n");
		}

		res.append(infoMap.size());
		res.append("\n");
		for(int key:infoMap.keySet()) {
			final Info info = infoMap.get(key); 
			res.append(key + "\t" + info.numA + "\t" + info.numT + "\t" + info.numG + "\t" + info.numC + "\t" + info.numN + "\t" + (1.0f * info.accQual / info.numQual));
			res.append("\n");
		}

		// update kmers, if necessary
		if (kmers.kvalue > 0) {
			res.append(kmers.toFormat());
		}

		return res.toString();
	}
}
