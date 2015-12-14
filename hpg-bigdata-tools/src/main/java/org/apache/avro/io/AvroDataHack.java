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
package org.apache.avro.io;

/**
 * Created by pawan on 19/11/15.
 */
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.IOException;
import java.util.*;

/**
 * Utilities for to compare and generate hashes of Avro objects.
 * <p/>
 * Copied and adapted from {@link BinaryData} and {@link org.apache.avro.generic.GenericData}
 * since these classes aren't designed to be extended.
 */
public final class AvroDataHack {

    private AvroDataHack() {

    }

    private static class Decoders {
        private final BinaryDecoder d1, d2;

        public Decoders() {
            this.d1 = new BinaryDecoder(new byte[0], 0, 0);
            this.d2 = new BinaryDecoder(new byte[0], 0, 0);
        }

        public void set(byte[] data1, int off1, int len1,
                        byte[] data2, int off2, int len2) {
            d1.setBuf(data1, off1, len1);
            d2.setBuf(data2, off2, len2);
        }

        public void clear() {
            d1.clearBuf();
            d2.clearBuf();
        }
    }

    private static final ThreadLocal<Decoders> DECODERS
            = new ThreadLocal<Decoders>() {
        @Override
        protected Decoders initialValue() {

            return new Decoders();
        }
    };

    public static class OrderedField {
        private Field field;
        private boolean ascendingOrder;

        public Field getField() {

            return field;
        }

        public OrderedField setField(Field field) {
            this.field = field;
            return this;
        }

        public boolean isAscendingOrder() {

            return ascendingOrder;
        }

        public OrderedField setAscendingOrder(boolean ascendingOrder) {
            this.ascendingOrder = ascendingOrder;
            return this;
        }

        @Override
        public int hashCode() {

            return HashCodeBuilder.reflectionHashCode(this);
        }

        @Override
        public boolean equals(Object o) {

            return EqualsBuilder.reflectionEquals(this, o);
        }

        @Override
        public String toString() {

            return ToStringBuilder.reflectionToString(this);
        }
    }

    public interface FieldFetcher {
        List<OrderedField> getFields(Schema schema);
    }


    public static int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2, Schema schema,
                              FieldFetcher fetcher) {
        Decoders decoders = DECODERS.get();
        decoders.set(b1, s1, l1, b2, s2, l2);
        try {
            return compare(decoders, schema, fetcher);
        } catch (IOException e) {
            throw new AvroRuntimeException(e);
        } finally {
            decoders.clear();
        }
    }


    public static int compare(Decoders d, Schema schema, FieldFetcher fetcher) throws IOException {
        Decoder d1 = d.d1;
        Decoder d2 = d.d2;
        List<OrderedField> orderedFields = fetcher.getFields(schema);
        Map<Field, Integer> fieldComparables = new HashMap<>();
        Map<Field, Boolean> fieldAscending = new HashMap<>();
        for (OrderedField field : orderedFields) {
            fieldComparables.put(field.getField(), null);
            fieldAscending.put(field.getField(), field.isAscendingOrder());
        }
        switch (schema.getType()) {
            case RECORD: {
                for (Field field : schema.getFields()) {
                    if (!fieldComparables.containsKey(field)) {
                        GenericDatumReader.skip(field.schema(), d1);
                        GenericDatumReader.skip(field.schema(), d2);
                        continue;
                    }
                    int compare = compare(d, field.schema(), fetcher);
                    if (!fieldAscending.get(field)) {
                        compare = -compare;
                    }
                    fieldComparables.put(field, compare);
                }
                for (OrderedField field : orderedFields) {
                    Integer compare = fieldComparables.get(field.getField());
                    if (compare != null && compare != 0) {
                        return compare;
                    }
                }
                return 0;
            }
            case ENUM:
            case INT: {
                int i1 = d1.readInt();
                int i2 = d2.readInt();
                return i1 == i2 ? 0 : (i1 > i2 ? 1 : -1);
            }
            case LONG: {
                long l1 = d1.readLong();
                long l2 = d2.readLong();
                return l1 == l2 ? 0 : (l1 > l2 ? 1 : -1);
            }
            case ARRAY: {
                long i = 0;                                 // position in array
                long r1 = 0, r2 = 0;                        // remaining in current block
                long l1 = 0, l2 = 0;                        // total array length
                while (true) {
                    if (r1 == 0) {                            // refill blocks(s)
                        r1 = d1.readLong();
                        if (r1 < 0) {
                            r1 = -r1;
                            d1.readLong();
                        }
                        l1 += r1;
                    }
                    if (r2 == 0) {
                        r2 = d2.readLong();
                        if (r2 < 0) {
                            r2 = -r2;
                            d2.readLong();
                        }
                        l2 += r2;
                    }
                    if (r1 == 0 || r2 == 0) {                  // empty block: done
                        return (l1 == l2) ? 0 : ((l1 > l2) ? 1 : -1);
                    }
                    long l = Math.min(l1, l2);
                    while (i < l) {                           // compare to end of block
                        int c = compare(d, schema.getElementType(), fetcher);
                        if (c != 0) {
                            return c;
                        }
                        i++;
                        r1--;
                        r2--;
                    }
                }
            }
            case MAP:
                throw new AvroRuntimeException("Can't compare maps!");
            case UNION: {
                int i1 = d1.readInt();
                int i2 = d2.readInt();
                if (i1 == i2) {
                    return compare(d, schema.getTypes().get(i1), fetcher);
                } else {
                    return i1 - i2;
                }
            }
            case FIXED: {
                int size = schema.getFixedSize();
                int c = compareBytes(d.d1.getBuf(), d.d1.getPos(), size,
                        d.d2.getBuf(), d.d2.getPos(), size);
                d.d1.skipFixed(size);
                d.d2.skipFixed(size);
                return c;
            }
            case STRING:
            case BYTES: {
                int l1 = d1.readInt();
                int l2 = d2.readInt();
                int c = compareBytes(d.d1.getBuf(), d.d1.getPos(), l1,
                        d.d2.getBuf(), d.d2.getPos(), l2);
                d.d1.skipFixed(l1);
                d.d2.skipFixed(l2);
                return c;
            }
            case FLOAT: {
                float f1 = d1.readFloat();
                float f2 = d2.readFloat();
                return (f1 == f2) ? 0 : ((f1 > f2) ? 1 : -1);
            }
            case DOUBLE: {
                double f1 = d1.readDouble();
                double f2 = d2.readDouble();
                return (f1 == f2) ? 0 : ((f1 > f2) ? 1 : -1);
            }
            case BOOLEAN:
                boolean b1 = d1.readBoolean();
                boolean b2 = d2.readBoolean();
                return (b1 == b2) ? 0 : (b1 ? 1 : -1);
            case NULL:
                return 0;
            default:
                throw new AvroRuntimeException("Unexpected schema to compare!");
        }
    }


    public static int compareBytes(byte[] b1, int s1, int l1,
                                   byte[] b2, int s2, int l2) {
        int end1 = s1 + l1;
        int end2 = s2 + l2;
        for (int i = s1, j = s2; i < end1 && j < end2; i++, j++) {
            int a = (b1[i] & 0xff);
            int b = (b2[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return l1 - l2;
    }

    public static int hashCode(Object o, Schema s, FieldFetcher fetcher) {
        if (o == null) {
            return 0;                      // incomplete datum
        }
        int hashCode = 1;

        List<OrderedField> orderedFields = fetcher.getFields(s);

        Set<Field> fieldComparables = new HashSet<>();
        for (OrderedField field : orderedFields) {
            fieldComparables.add(field.getField());
        }
        switch (s.getType()) {
            case RECORD:
                for (Field f : s.getFields()) {

                    if (!fieldComparables.contains(f)) {
                        continue;
                    }

                    hashCode = hashCodeAdd(hashCode,
                            getField(o, f.pos()), f.schema(), fetcher);
                }
                return hashCode;
            case ARRAY:
                Collection<?> a = (Collection<?>)o;
                Schema elementType = s.getElementType();
                for (Object e : a) {
                    hashCode = hashCodeAdd(hashCode, e, elementType, fetcher);
                }
                return hashCode;
            case UNION:
                throw new UnsupportedOperationException();
//                return hashCode(o, s.getTypes().get(resolveUnion(s, o)));
            case ENUM:
                return s.getEnumOrdinal(o.toString());
            case NULL:
                return 0;
            case STRING:
                return (o instanceof Utf8 ? o : new Utf8(o.toString())).hashCode();
            default:
                return o.hashCode();
        }
    }

    public static Object getField(Object record, int position) {

        return ((IndexedRecord)record).get(position);
    }

    public static int hashCodeAdd(int hashCode, Object o, Schema s, FieldFetcher fetcher) {
        return 31 * hashCode + hashCode(o, s, fetcher);
    }

    public static int compare(Object o1, Object o2, Schema s, FieldFetcher fetcher) {
        if (o1 == o2) {
            return 0;
        }
        List<OrderedField> orderedFields = fetcher.getFields(s);
        Map<Field, Integer> fieldComparables = new HashMap<>();
        Map<Field, Boolean> fieldAscending = new HashMap<>();
        for (OrderedField field : orderedFields) {
            fieldComparables.put(field.getField(), null);
            fieldAscending.put(field.getField(), field.isAscendingOrder());
        }
        switch (s.getType()) {
            case RECORD:

                for (Field field : s.getFields()) {
                    if (!fieldComparables.containsKey(field)) {
                        continue;
                    }
                    int pos = field.pos();
                    int compare = compare(getField(o1, pos), getField(o2, pos),
                            field.schema(), fetcher);
                    if (!fieldAscending.get(field)) {
                        compare = -compare;
                    }
                    fieldComparables.put(field, compare);
                }
                for (OrderedField field : orderedFields) {
                    Integer compare = fieldComparables.get(field.getField());
                    if (compare != null && compare != 0) {
                        return compare;
                    }
                }
                return 0;
            case ENUM:
                return s.getEnumOrdinal(o1.toString()) - s.getEnumOrdinal(o2.toString());
            case ARRAY:
                Collection a1 = (Collection)o1;
                Collection a2 = (Collection)o2;
                Iterator e1 = a1.iterator();
                Iterator e2 = a2.iterator();
                Schema elementType = s.getElementType();
                while (e1.hasNext() && e2.hasNext()) {
                    int compare = compare(e1.next(), e2.next(), elementType, fetcher);
                    if (compare != 0) {
                        return compare;
                    }
                }
                return e1.hasNext() ? 1 : (e2.hasNext() ? -1 : 0);
            case MAP:
                throw new AvroRuntimeException("Can't compare maps!");
            case UNION:
                throw new UnsupportedOperationException();
/*                int i1 = resolveUnion(s, o1);
                int i2 = resolveUnion(s, o2);
                return (i1 == i2)
                        ? compare(o1, o2, s.getTypes().get(i1))
                        : i1 - i2;*/
            case NULL:
                return 0;
            case STRING:
                Utf8 u1 = o1 instanceof Utf8 ? (Utf8)o1 : new Utf8(o1.toString());
                Utf8 u2 = o2 instanceof Utf8 ? (Utf8)o2 : new Utf8(o2.toString());
                return u1.compareTo(u2);
            default:
                return ((Comparable)o1).compareTo(o2);
        }
    }
}
