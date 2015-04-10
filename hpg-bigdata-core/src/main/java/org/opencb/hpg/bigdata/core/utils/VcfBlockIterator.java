/**
 * 
 */
package org.opencb.hpg.bigdata.core.utils;

import htsjdk.tribble.readers.LineIterator;
import htsjdk.variant.vcf.VCFHeader;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.CharBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.directory.api.util.exception.NotImplementedException;
import org.opencb.hpg.bigdata.core.converters.FullVCFCodec;

/**
 * @author mh719
 *
 */
public class VcfBlockIterator implements AutoCloseable,Iterator<List<CharBuffer>>, Iterable<List<CharBuffer>> {
	private static final long DEFAULT_64KB_BLOCK = 64l*1024l;
	
	private final File file;
	private final InputStream in;
	private final LineIterator iter;
	
	private final AtomicLong charBlockSize = new AtomicLong(DEFAULT_64KB_BLOCK);

	private final VCFHeader header;
	
	public VcfBlockIterator(File vcfFile) throws IOException {
		this(vcfFile,new FullVCFCodec());
	}
	
	public VcfBlockIterator(File vcfFile, FullVCFCodec codec) throws IOException {
		this.file = vcfFile;
		this.in = buildInputStream(this.file);
		this.iter = codec.makeSourceFromStream(this.in);
		this.header = (VCFHeader) codec.readActualHeader(this.iter);
	}
	
	public VcfBlockIterator(InputStream in, FullVCFCodec codec) throws IOException {
		this.file = null;
		this.in = in;
		this.iter = codec.makeSourceFromStream(this.in);
		this.header = (VCFHeader) codec.readActualHeader(this.iter);
	}
	
	public VCFHeader getHeader() {
		return this.header;
	}
	
	@Override
	public List<CharBuffer> next() {
		long cnt = 0l;
		List<CharBuffer> next = new LinkedList<CharBuffer>(); // linked list faster at creation time
		while(iter.hasNext() && cnt < this.charBlockSize.get()){
			String line = iter.next();
			CharBuffer buff = CharBuffer.wrap(line.toCharArray());
			next.add(buff);
			cnt += buff.length();
		}
		return next;
	}

	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	protected InputStream  buildInputStream(File inFile) throws IOException {
		InputStream  in = new FileInputStream(inFile);
		String name = inFile.getName();
		String ext = FilenameUtils.getExtension(name);
		switch (ext) {
		case "gz":
		case "gzip":
			in = new GZIPInputStream(in);
			break;
		case "vcf":
		case "txt":
		case "tsv":
			//nothing to do
			break;
		default:
			throw new NotImplementedException(String.format("Compression extension %s not yet supported!!!", ext));
		}
		return new BufferedInputStream(in);
	}

	@Override
	public void close() throws Exception {
		this.in.close();
	}

	@Override
	public void remove() {
		throw new NotImplementedException();
	}

	@Override
	public Iterator<List<CharBuffer>> iterator() {
		return this;
	}	
}
