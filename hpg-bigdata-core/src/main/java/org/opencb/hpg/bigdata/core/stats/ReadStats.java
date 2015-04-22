package org.opencb.hpg.bigdata.core.stats;

import java.util.HashMap;
import java.util.List;

import org.ga4gh.models.Read;

public class ReadStats {

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
	public int accSeqQual;
	public HashMap<Integer, Integer> lengthMap;
	public HashMap<Integer, Info> infoMap;

	public ReadKmers kmers;

	public ReadStats() {
		numSeqs = 0;
		numA = 0;
		numT = 0;
		numG = 0;
		numC = 0;
		numN = 0;
		minSeqLength = Integer.MAX_VALUE;
		maxSeqLength = 0;
		accSeqQual = 0;
		lengthMap = new HashMap<Integer, Integer>();
		infoMap = new HashMap<Integer, Info>();
		kmers = new ReadKmers();
	}

	public ReadStats(ReadStats readStats) {
		set(readStats);
	}

	public void set(ReadStats readStats) {
		if (readStats != null) {
			numSeqs = readStats.numSeqs;
			numA = readStats.numA;
			numT = readStats.numT;
			numG = readStats.numG;
			numC = readStats.numC;
			numN = readStats.numN;
			minSeqLength = readStats.minSeqLength;
			maxSeqLength = readStats.maxSeqLength;
			accSeqQual = readStats.accSeqQual;
			lengthMap = readStats.lengthMap;
			infoMap = readStats.infoMap;
			kmers = readStats.kmers;
		}
	}

	public void update(ReadStats stats) {
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
		updateBySequences(read.getSequence().toString(), read.getQuality().toString());
	}

	public void updateBySequences(final String sequence, final String quality) {
		numSeqs++;
		Info info = null;
		//final String read = fastqRecord.getReadString();
		//final String quality = fastqRecord.getBaseQualityString();
		final int len = sequence.length();

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
			switch (sequence.charAt(i)) {
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

		// update kmers, if necessary
		if (kmers.kvalue > 0) {
			kmers.updateBySequence(sequence, kmers.kvalue);
		}
	}

	public void updateBySequences2(final String sequence, final List<Integer> quality) {
		
		numSeqs++;
		Info info = null;
		//final String read = fastqRecord.getReadString();
		//final String quality = fastqRecord.getBaseQualityString();
		final int len = sequence.length();

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
			qual = quality.get(i);
			accQual += qual;
			info.numQual++;
			info.accQual += qual;

			// nucleotide content
			switch (sequence.charAt(i)) {
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

		// update kmers, if necessary
		if (kmers.kvalue > 0) {
			kmers.updateBySequence(sequence, kmers.kvalue);
		}
	}

	public String toJSON() {
		StringBuilder res = new StringBuilder();
		res.append("{");
		res.append("\"num_reads\": " + numSeqs);
		res.append(", \"num_A\": " + numA);
		res.append(", \"num_T\": " + numT);
		res.append(", \"num_G\": " + numG);
		res.append(", \"num_C\": " + numC);
		res.append(", \"num_N\": " + numN);
		
		int mean_len = (numA + numT + numG + numC + numN) / numSeqs;
		res.append(", \"min_length\": " + minSeqLength);
		res.append(", \"mean_length\": " + mean_len);
		res.append(", \"max_length\": " + maxSeqLength);
		
		res.append(", \"mean_qual\": " + accSeqQual / numSeqs / mean_len);
		
		int i, size = lengthMap.size();
		res.append(", \"length_map_size\": " + size);
		res.append(", \"length_map_values\": [");

		i = 0;
		for(int key:lengthMap.keySet()) {
			res.append("[" + key + ", " + lengthMap.get(key) + "]");
			if (++i < size) res.append(", ");
		}
		res.append("]");

		size = infoMap.size();
		res.append(", \"info_map_size\": " + size);
		res.append(", \"info_map_values\": [");

		i = 0;
		for(int key:infoMap.keySet()) {
			final Info info = infoMap.get(key); 
			res.append("[" + key + ", " + info.numA + ", " + info.numT + ", " + info.numG + ", " + info.numC + ", " + info.numN + ", " + (1.0f * info.accQual / info.numQual) + "]");
			if (++i < size) res.append(", ");
		}
		res.append("]");

		// update kmers, if necessary
		if (kmers.kvalue > 0) {
			res.append(", \"kmers\": " + kmers.toJSON());
		}
		res.append("}");

		return res.toString();
	}
}
