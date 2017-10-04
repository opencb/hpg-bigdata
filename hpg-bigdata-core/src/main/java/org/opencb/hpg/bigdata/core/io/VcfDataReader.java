package org.opencb.hpg.bigdata.core.io;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import org.opencb.biodata.tools.variant.VcfFileReader;
import org.opencb.commons.io.DataReader;

import java.io.IOException;
import java.util.List;

/**
 * Created by jtarraga on 04/10/17.
 */
public class VcfDataReader implements DataReader<VariantContext> {
    private String vcfFilename;
    private VcfFileReader vcfFileReader;
    private VCFHeader vcfHeader;
    private boolean openned;

    public VcfDataReader(String vcfFilename) {
        openned = false;
        this.vcfFilename = vcfFilename;
        open();
    }

    @Override
    public boolean open() {
        if (!openned) {
            // VCF reader
            vcfFileReader = new VcfFileReader();
            openned = true;
            try {
                vcfFileReader.open(vcfFilename);
                vcfHeader = vcfFileReader.getVcfHeader();
            } catch (IOException e) {
                e.printStackTrace();
                openned = false;
            }
        }
        return openned;
    }

    @Override
    public List<VariantContext> read(int batchSize) {
        return vcfFileReader.read(batchSize);
    }

    @Override
    public boolean close() {
        openned = false;
        return vcfFileReader.close();
    }

    public VCFHeader vcfHeader() {
        return vcfHeader;
    }
}
