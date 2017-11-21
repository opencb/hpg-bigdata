package org.opencb.hpg.bigdata.core.lib;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.opencb.biodata.models.variant.Variant;

import java.io.IOException;
import java.util.Iterator;

public class VariantDatasetIterator implements Iterator<Variant> {

    private Iterator<String> strIterator;

    private ObjectReader objectReader;

    public VariantDatasetIterator(VariantDataset variantDataset) {
        strIterator = variantDataset.toJSON().toLocalIterator();

        // We use ObjectReader for performance reasons.
        ObjectMapper objMapper = new ObjectMapper();
        objectReader = objMapper.readerFor(Variant.class);
    }

    @Override
    public boolean hasNext() {
        return strIterator.hasNext();
    }

    @Override
    public Variant next() {
        try {
            return objectReader.readValue(strIterator.next());
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
