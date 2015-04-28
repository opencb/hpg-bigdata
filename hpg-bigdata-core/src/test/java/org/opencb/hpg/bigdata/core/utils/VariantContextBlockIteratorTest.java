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

/**
 * 
 */
package org.opencb.hpg.bigdata.core.utils;

import static org.junit.Assert.assertEquals;
import htsjdk.tribble.readers.LineIterator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;

import java.io.InputStream;
import java.nio.CharBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.hpg.bigdata.core.converters.FullVCFCodec;

/**
 * @author mh719
 *
 */
public class VariantContextBlockIteratorTest {
	
	List<CharBuffer> rows;
	FullVCFCodec codec;
	VCFHeader header;
	private VCFHeaderVersion version;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		rows = new LinkedList<CharBuffer>();
		String inFile = "/"+this.getClass().getName().replaceAll("\\.", "/")+".vcf";
		FullVCFCodec codec = new FullVCFCodec();
		try(InputStream in = getStream(inFile);){
			LineIterator iter = codec.makeSourceFromStream(in);
			this.header = (VCFHeader) codec.readActualHeader(iter);
			this.version = codec.getVCFHeaderVersion();
			
		}
		try(InputStream in = getStream(inFile);){
			List<String> lines = IOUtils.readLines(in);
			for(String l : lines){
				if(!l.startsWith("#")){
					rows.add(CharBuffer.wrap(l));
				}
			}
		}
//		rows.add(CharBuffer.wrap("20	60479	rs149529999	C	T 	100	PASS	ERATE=0.0005;RSQ=0.8951;LDAF=0.0021;	GT:DS:GL	0|0:0.000:-0.19,-0.46,-2.68	0|0:0.000:-0.01,-1.85,-5.00"));
	}
	
	private InputStream getStream(String inFile){
		return getClass().getResourceAsStream(inFile);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		rows = null;
		codec = null;
		this.header = null;
		this.version = null;
	}

	@Test
	public void test() {
		FullVCFCodec fc = new FullVCFCodec();
		fc.setVCFHeader(this.header, this.version);
		VariantContextBlockIterator iter = new VariantContextBlockIterator(fc);
		List<VariantContext> ctxList = iter.convert(this.rows);
		assertEquals("Assume the same list size", this.rows.size(), ctxList.size());
	}

}
