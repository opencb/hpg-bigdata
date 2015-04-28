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

package org.opencb.hpg.bigdata.core.stats;

import java.util.HashMap;
import java.util.List;

import org.ga4gh.models.CigarUnit;
import org.ga4gh.models.LinearAlignment;
import org.ga4gh.models.ReadAlignment;

public class ReadAlignmentStats {

	public int numMapped;
	public int numUnmapped;
	public int numPaired;
	public int numMappedFirst;
	public int numMappedSecond;

	public int NM;
	
	public int numHardC;
	public int numSoftC;
	public int numIn;
	public int numDel;
	public int numPad;
	public int numSkip;
	
	public int accMappingQuality;
	public HashMap<Integer, Integer> mappingQualityMap;

	public int accInsert;
	public HashMap<Integer, Integer> insertMap;

//	public long pos;
//	public List<CigarUnit> cigar;

	public ReadStats readStats;


	public ReadAlignmentStats () {
		numMapped = 0;
		numUnmapped = 0;
		numPaired = 0;
		numMappedFirst = 0;
		numMappedSecond = 0;

		NM = 0;
		
		numHardC = 0;
		numSoftC = 0;
		numIn = 0;
		numDel = 0;
		numPad = 0;
		numSkip = 0;

		accMappingQuality = 0;
		mappingQualityMap = new HashMap<Integer, Integer> ();

		accInsert = 0;
		insertMap = new HashMap<Integer, Integer> ();

//		pos = 0;
//		cigar = null;
		
		readStats = new ReadStats();
	}

	public void update(ReadAlignmentStats stats) {
		int value;

		numMapped += stats.numMapped;
		numUnmapped += stats.numUnmapped;
		numPaired += stats.numPaired;
		numMappedFirst += stats.numMappedFirst;
		numMappedSecond += stats.numMappedSecond;

		NM += stats.NM;
		
		numHardC += stats.numHardC;
		numSoftC += stats.numSoftC;
		numIn += stats.numIn;
		numDel += stats.numDel;
		numPad += stats.numPad;
		numSkip += stats.numSkip;

		accMappingQuality += stats.accMappingQuality;
		for(int key:stats.mappingQualityMap.keySet()) {
			value = stats.mappingQualityMap.get(key);
			if (mappingQualityMap.containsKey(key)) {
				value += mappingQualityMap.get(key);
			}
			mappingQualityMap.put(key, value);
		}

		accInsert += stats.accInsert;
		for(int key:stats.insertMap.keySet()) {
			value = stats.insertMap.get(key);
			if (insertMap.containsKey(key)) {
				value += insertMap.get(key);
			}
			insertMap.put(key, value);
		}

		
		readStats.update(stats.readStats);
	}

	public void updateByReadAlignment(ReadAlignment ra) {
		if (ra.getAlignment() != null) {
			// mapped
			numMapped++;
			
			int value;
			LinearAlignment la = (LinearAlignment) ra.getAlignment();

			//System.out.println("chr " + la.getPosition().getReferenceName().toString() + " : " + la.getPosition().getPosition() + ", " + ra.getAlignedSequence().length());
			
			// num. mismatches
			if (ra.getInfo() != null) {
				List<CharSequence> values = ra.getInfo().get("NM");
				if (values != null) {
					NM = Integer.parseInt(values.get(1).toString());
				}
			}
			
//			pos = la.getPosition().getPosition();
//			cigar = la.getCigar();
			
			// clipping, indels...
			List<CigarUnit> cigar = la.getCigar();
			if (cigar != null) {
				boolean hard = false, soft = false, in = false, del = false, pad = false, skip = false;
				for (CigarUnit element: cigar) {
					switch(element.getOperation()) {
					case CLIP_HARD: 
						hard = true;
						break;
					case CLIP_SOFT:
						soft = true;
						break;
					case DELETE:
						del = true;
						break;
					case INSERT:
						in = true;
						break;
					case PAD:
						pad = true;
						break;
					case SKIP:
						skip = true;
						break;
					default:
						break;
					}
				}
				if (hard) numHardC++;
				if (soft) numSoftC++;
				if (in) numIn++;
				if (del) numDel++;
				if (pad) numPad++;
				if (skip) numSkip++;
			}
					
			// paired, first, second
			if (ra.getProperPlacement()) {
				numPaired++;

				// insert
				int insert = Math.abs(ra.getFragmentLength());
				value = 1;
				accInsert += insert;
				if (insertMap.containsKey(insert)) {
					value += insertMap.get(insert);
				}
				insertMap.put(insert, value);

			}
			if (ra.getReadNumber() == 0) {
				numMappedFirst++;
			}
			if (ra.getReadNumber() == ra.getNumberReads() - 1) {
				numMappedSecond++;
			}

			// mapping quality
			int mappingQuality = la.getMappingQuality();
			value = 1;
			accMappingQuality += mappingQuality;
			if (mappingQualityMap.containsKey(mappingQuality)) {
				value += mappingQualityMap.get(mappingQuality);
			}
			mappingQualityMap.put(mappingQuality, value);

		} else {
			// unmapped
			numUnmapped++;
		}

		readStats.updateBySequences2(ra.getAlignedSequence().toString(), ra.getAlignedQuality());		
	}

	public String toJSON() {
		int i, size;
		StringBuilder res = new StringBuilder();
		res.append("{");
		res.append("\"num_mapped\": " + numMapped);
		res.append(", \"num_unmapped\": " + numUnmapped);
		res.append(", \"num_paired\": " + numPaired);
		res.append(", \"num_mapped_first\": " + numMappedFirst);
		res.append(", \"num_mapped_second\": " + numMappedSecond);

		res.append(", \"num_mismatches\": " + NM);

		res.append(", \"num_hard_clipping\": " + numHardC);
		res.append(", \"num_soft_clipping\": " + numSoftC);
		res.append(", \"num_insertion\": " + numIn);
		res.append(", \"num_deletion\": " + numDel);
		res.append(", \"num_padding\": " + numPad);
		res.append(", \"num_skip\": " + numSkip);

		size = mappingQualityMap.size();
		res.append(", \"mapping_quality_mean\": " + (accMappingQuality / numMapped));
		res.append(", \"mapping_quality_map_size\": " + size);
		res.append(", \"mapping_quality_map_values\": [");
		i = 0;
		for(int key:mappingQualityMap.keySet()) {
			res.append("[" + key + ", " + mappingQualityMap.get(key) + "]");
			if (++i < size) res.append(", ");
		}
		res.append("]");

		if (numPaired > 0) {
			size = insertMap.size();
			res.append(", \"insert_mean\": " + (accInsert / numPaired));
			res.append(", \"insert_map_size\": " + size);
			res.append(", \"insert_map_values\": [");
			i = 0;
			for(int key:insertMap.keySet()) {
				res.append("[" + key + ", " + insertMap.get(key) + "]");
				if (++i < size) res.append(", ");
			}
			res.append("]");
		}

		res.append(", \"read_stats\": " + readStats.toJSON());
		res.append("}");
		return res.toString();
	}

}
