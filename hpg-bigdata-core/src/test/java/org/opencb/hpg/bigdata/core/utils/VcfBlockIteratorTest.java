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
	 * Test method for {@link org.opencb.hpg.bigdata.core.utils.VcfBlockIterator#hasNext()}.
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
