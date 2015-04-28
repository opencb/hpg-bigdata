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

import java.io.InputStream;
import java.nio.CharBuffer;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.hpg.bigdata.core.converters.FullVCFCodec;
import org.opencb.hpg.bigdata.core.io.VcfBlockIterator;

/**
 * @author mh719
 *
 */
public class VcfBlockIteratorTest {
	
	private String inFile;
	private int VCF_VAR_LINE_COUNT = 10;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		this.inFile = "/"+this.getClass().getName().replaceAll("\\.", "/")+".vcf";
	}
	
	private InputStream getStream(){
		return getClass().getResourceAsStream(inFile);
	}
	
	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link org.opencb.hpg.bigdata.core.io.VcfBlockIterator#hasNext()}.
	 * @throws Exception 
	 */
	@Test
	public void testIterator() throws Exception {
		int cnt = 0;
		try(
			InputStream in = getStream();
			VcfBlockIterator iter = new VcfBlockIterator(in, new FullVCFCodec());){
			for(List<CharBuffer> s : iter){
				cnt += s.size();
			}
		}
		assertEquals("Number of Variant row don't match!!!", VCF_VAR_LINE_COUNT, cnt);
	}

}
