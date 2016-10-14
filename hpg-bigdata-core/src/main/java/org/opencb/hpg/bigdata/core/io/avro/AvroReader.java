package org.opencb.hpg.bigdata.core.io.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.opencb.commons.io.DataReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by jtarraga on 13/10/16.
 */
public class AvroReader<T> implements DataReader<T> {

    private DatumReader<T> datumReader;
    private DataFileStream<T> dataFileStream;
    private InputStream inputStream;

    public AvroReader(InputStream inputStream, Schema schema) {
        this.inputStream = inputStream;
        datumReader = new SpecificDatumReader<>(schema);
    }

    @Override
    public boolean open() {
        try {
            dataFileStream = new DataFileStream<>(inputStream, datumReader);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean close() {
        try {
            dataFileStream.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean pre() {
        return true;
    }

    @Override
    public boolean post() {
        return true;
    }

    @Override
    public List<T> read() {
        if (dataFileStream.hasNext()) {
            T item = null;
            try {
                item = dataFileStream.next(item);
                return Collections.singletonList(item);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }
        return null;
    }

    @Override
    public List<T> read(int batchSize) {
        int counter = 0;
        T item = null;
        List<T> list = new ArrayList<T>();
        while (dataFileStream.hasNext()) {
            try {
                item = dataFileStream.next(item);
                list.add(item);

                if ((++counter) >= batchSize) {
                    return list;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return list;
    }
}
