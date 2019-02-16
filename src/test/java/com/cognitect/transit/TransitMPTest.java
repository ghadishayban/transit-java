// Copyright 2014 Cognitect. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS-IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cognitect.transit;

import com.cognitect.transit.impl.JsonParser;
import com.cognitect.transit.impl.Tag;
import com.cognitect.transit.impl.WriteCache;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.commons.codec.binary.Base64;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class TransitMPTest extends TestCase {

    public TransitMPTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TransitMPTest.class);
    }

    static Value asValue(Object o) {

        if (o == null) {
            return ValueFactory.newNil();
        } else if (o instanceof Integer) {
            return ValueFactory.newInteger((Integer) o);
        } else if (o instanceof Long) {
            return ValueFactory.newInteger((Long) o);
        } else if (o instanceof Double) {
            return ValueFactory.newFloat((Double) o);
        } else if (o instanceof String) {
            return ValueFactory.newString((String) o);
        } else if (o instanceof Boolean) {
            return ValueFactory.newBoolean((Boolean) o);
        } else if (o instanceof List) {
            Value[] vs = ((List<Object>) o)
                    .stream()
                    .map(TransitMPTest::asValue)
                    .toArray(Value[]::new);
            return ValueFactory.newArray(vs, true);
        } else if (o instanceof Map) {
            List<Value> vs = new ArrayList<>();
            ((Map<Object,Object>) o).entrySet().forEach(e -> {
                vs.add(asValue(e.getKey()));
                vs.add(asValue(e.getValue()));
            });
            return ValueFactory.newMap(vs.stream().toArray(Value[]::new), true);
        } else if (o.getClass().isArray()) {
            // convert all to list
            Class comp = o.getClass().getComponentType();
            if (comp.isPrimitive()) {
                if (comp.equals(Long.TYPE)) {
                    List ls = Arrays.stream((long[]) o).boxed().collect(Collectors.toList());
                    return asValue(ls);
                } else if (comp.equals(Integer.TYPE)) {
                    List ls = Arrays.stream((int[]) o).boxed().collect(Collectors.toList());
                    return asValue(ls);
                } else {
                    throw new UnsupportedOperationException("cannot convert array of " + comp);
                }
            } else {
                List ls = Arrays.stream((Object[]) o).collect(Collectors.toList());
                return asValue(ls);
            }
        } else {
            throw new UnsupportedOperationException("cannot convert " + o.getClass());
        }
    }

    static Map mapOf(Object... kvs) {
        Map m = new HashMap();
        for (int i = 0; i < kvs.length; i+=2){
            m.put(kvs[i], kvs[i+1]);
        }
        return m;
    }
    static List listOf(Object... vs) {
        List l = new ArrayList();
        for (Object v : vs) {
            l.add(v);
        }
        return l;
    }

    // Reading
    public Reader readerOf(Object... things) throws IOException {
        MessageBufferPacker msgpack = MessagePack.newDefaultBufferPacker();

        for (Object o : things) {
            msgpack.packValue(asValue(o));
        }

        InputStream in = new ByteArrayInputStream(msgpack.toByteArray());
        return TransitFactory.reader(TransitFactory.Format.MSGPACK, in);

    }

    public void testReadString() throws IOException {

        assertEquals("foo", readerOf("foo").read());
        assertEquals("~foo", readerOf("~~foo").read());
        assertEquals("`foo", readerOf("~`foo").read());
        assertEquals("foo", ((Tag)readerOf("~#foo").read()).getValue());
        assertEquals("^foo", readerOf("~^foo").read());
    }

    public void testReadBoolean() throws IOException {

        assertTrue((Boolean)readerOf("~?t").read());
        assertFalse((Boolean) readerOf("~?f").read());

        Map thing = mapOf("~?t", 1, "~?f", 2);

        Map m = readerOf(thing).read();
        assertEquals(1L, m.get(true));
        assertEquals(2L, m.get(false));
    }

    public void testReadNull() throws IOException {
        assertNull(readerOf("~_").read());
    }

    public void testReadKeyword() throws IOException {

        Object v = readerOf("~:foo").read();
        assertEquals(":foo", v.toString());

        List thing = listOf("~:foo",
                "^" + (char) WriteCache.BASE_CHAR_IDX,
                "^" + (char) WriteCache.BASE_CHAR_IDX);

        List v2 = readerOf(thing).read();
        assertEquals(":foo", v2.get(0).toString());
        assertEquals(":foo", v2.get(1).toString());
        assertEquals(":foo", v2.get(2).toString());

    }

    public void testReadInteger() throws IOException {

        Reader r = readerOf("~i42");
        assertEquals(42L, (long) r.read());

        r = readerOf("~n4256768765123454321897654321234567");
        assertEquals(0, (new BigInteger("4256768765123454321897654321234567")).compareTo((BigInteger)r.read()));
    }

    public void testReadDouble() throws IOException {

        assertEquals(Double.parseDouble("42.5"), readerOf("~d42.5").read());
    }

    public void testReadBigDecimal() throws IOException {

        assertEquals(0, (new BigDecimal("42.5")).compareTo((BigDecimal)readerOf("~f42.5").read()));
    }

    private long readTimeString(String timeString) throws IOException {
        return ((Date)readerOf("~t" + timeString).read()).getTime();
    }

    private SimpleDateFormat formatter(String formatString) {

        SimpleDateFormat df = new SimpleDateFormat(formatString);
        df.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));
        return df;
    }

    private void assertReadsFormat(String formatString) throws Exception {

        Date d = new Date();
        SimpleDateFormat df = formatter(formatString);
        String ds = df.format(d);
        assertEquals(df.parse(ds).getTime(), readTimeString(ds));
    }

    public void testReadTime() throws Exception {

        Date d = new Date();
        final long t = d.getTime();
        String timeString = JsonParser.getDateTimeFormat().format(d);

        assertEquals(t, readTimeString(timeString));

        assertReadsFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        assertReadsFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        assertReadsFormat("yyyy-MM-dd'T'HH:mm:ss.SSS-00:00");

        Map thing = new HashMap();
        thing.put("~#m", t);

        assertEquals(t, ((Date)readerOf(thing).read()).getTime());
    }

    public void testReadUUID() throws IOException {

        UUID uuid = UUID.randomUUID();
        final long hi64 = uuid.getMostSignificantBits();
        final long lo64 = uuid.getLeastSignificantBits();

        assertEquals(0, uuid.compareTo((UUID)readerOf("~u" + uuid.toString()).read()));

        List thing = listOf("~#u", listOf(hi64, lo64));

        assertEquals(0, uuid.compareTo((UUID)readerOf(thing).read()));
    }

    public void testReadURI() throws URISyntaxException, IOException {

        URI uri = TransitFactory.uri("http://www.foo.com");

        assertEquals(0, uri.compareTo((URI)readerOf("~rhttp://www.foo.com").read()));
    }

    public void testReadSymbol() throws IOException {

        Reader r = readerOf("~$foo");
        Object v = r.read();
        assertEquals("foo", v.toString());
    }

    public void testReadCharacter() throws IOException {

        assertEquals('f', (char) readerOf("~cf").read());
    }

    // Binary data tests

    public void testReadBinary() throws IOException {

        byte[] bytes = "foobarbaz".getBytes();
        byte[] encodedBytes = Base64.encodeBase64(bytes);
        byte[] decoded = readerOf("~b" + new String(encodedBytes)).read();

        assertEquals(bytes.length, decoded.length);

        boolean same = true;
        for(int i=0;i<bytes.length;i++) {
            if(bytes[i]!=decoded[i])
                same = false;
        }

        assertTrue(same);
    }

    public void testReadUnknown() throws IOException {

        assertEquals(TransitFactory.taggedValue("j", "foo"), readerOf("~jfoo").read());

        final List l = Arrays.asList(1L, 2L);

        Map thing = mapOf("~#point", l);

        assertEquals(TransitFactory.taggedValue("point", l), readerOf(thing).read());
    }

    public void testReadArray() throws IOException {
        long[] thing = {1L, 2L, 3L};

        List l = readerOf(thing).read();

        assertTrue(l instanceof ArrayList);
        assertEquals(3, l.size());

        assertEquals(1L, l.get(0));
        assertEquals(2L, l.get(1));
        assertEquals(3L, l.get(2));
    }

    public void testReadArrayWithNestedDoubles() throws IOException {
        List thing = listOf(-3.14159, 3.14159, 4.0E11, 2.998E8, 6.626E-34);

        List l = readerOf(thing).read();

        for(int i = 0; i < l.size(); i++) {
            assertEquals(l.get(i), thing.get(i));
        }
    }

    public void testReadArrayWithNested() throws IOException {

        Date d = new Date();
        final String t = JsonParser.getDateTimeFormat().format(d);

        List thing = listOf("~:foo",
                "~t" + t,
                "~?t");


        List l = readerOf(thing).read();

        assertEquals(3, l.size());

        assertEquals(":foo", l.get(0).toString());
        assertEquals(d.getTime(), ((Date)l.get(1)).getTime());
        assertTrue((Boolean) l.get(2));

        final Date da[] = {new Date(-6106017600000L),
                           new Date(0),
                           new Date(946728000000L),
                           new Date(1396909037000L)};

        List dates = listOf(
                "~t" + JsonParser.getDateTimeFormat().format(da[0]),
                "~t" + JsonParser.getDateTimeFormat().format(da[1]),
                "~t" + JsonParser.getDateTimeFormat().format(da[2]),
                "~t" + JsonParser.getDateTimeFormat().format(da[3]));

        l = readerOf(dates).read();

        for (int i = 0; i < l.size(); i++) {
            Date date = (Date)l.get(i);
            assertEquals(date, da[i]);
        }
    }

    public void testReadMap() throws IOException {

        Map thing = mapOf("a", 2, "b", 4);

        Map m = readerOf(thing).read();

        assertEquals(2, m.size());

        assertEquals(2L, m.get("a"));
        assertEquals(4L, m.get("b"));
    }

    public void testReadMapWithNested() throws IOException {

        final String uuid = UUID.randomUUID().toString();

        Map thing = mapOf("a", "~:foo", "b", "~u" + uuid);

        Map m = readerOf(thing).read();

        assertEquals(2, m.size());

        assertEquals(":foo", m.get("a").toString());
        assertEquals(uuid, m.get("b").toString());
    }

    public void testReadSet() throws IOException {

        final int[] ints = {1,2,3};

        Map thing = mapOf( "~#set", ints);

        Set s = readerOf(thing).read();

        assertEquals(3, s.size());

        assertTrue(s.contains(1L));
        assertTrue(s.contains(2L));
        assertTrue(s.contains(3L));
    }

    public void testReadList() throws IOException {
        final int[] ints = {1,2,3};

        Map thing = mapOf("~#list", ints);

        List l = readerOf(thing).read();

        assertTrue(l instanceof LinkedList);
        assertEquals(3, l.size());

        assertEquals(1L, l.get(0));
        assertEquals(2L, l.get(1));
        assertEquals(3L, l.get(2));
    }

    public void testReadRatio() throws IOException {
        final String[] ratioRep = {"~n1", "~n2"};

        Map thing = mapOf("~#ratio", ratioRep);

        Ratio r = readerOf(thing).read();

        assertEquals(BigInteger.valueOf(1), r.getNumerator());
        assertEquals(BigInteger.valueOf(2), r.getDenominator());
        assertEquals(0.5d, r.getValue().doubleValue(), 0.01d);
    }

    public void testReadCmap() throws IOException {
        final String[] ratioRep = {"~n1", "~n2"};
        final int[] mints = {1,2,3};

        final Map ratio = mapOf("~#ratio", ratioRep);

        final Map list = mapOf("~#list", mints);

        final List things = listOf(ratio, 1, list, 2);

        final Map thing = mapOf("~#cmap", things);

        Map m = readerOf(thing).read();

        assertEquals(2, m.size());

        Iterator<Map.Entry> i = m.entrySet().iterator();
        while(i.hasNext()) {
            Map.Entry e = i.next();
            if((Long)e.getValue() == 1L) {
                Ratio r = (Ratio)e.getKey();
                assertEquals(BigInteger.valueOf(1), r.getNumerator());
                assertEquals(BigInteger.valueOf(2), r.getDenominator());
            }
            else if((Long)e.getValue() == 2L) {
                List l = (List)e.getKey();
                assertEquals(1L, l.get(0));
                assertEquals(2L, l.get(1));
                assertEquals(3L, l.get(2));
            }
        }
    }

    public void testReadMany() throws IOException {

        Reader r = readerOf(true, null, false, "foo", 42.2, 42);
        assertTrue((Boolean)r.read());
        assertNull(r.read());
        assertFalse((Boolean) r.read());
        assertEquals("foo", r.read());
        assertEquals(42.2, r.read());
        assertEquals(42L, (long) r.read());
    }

    public void testWriteReadTime() throws Exception {

        final Date da[] = {new Date(-6106017600000l),
                new Date(0),
                new Date(946728000000l),
                new Date(1396909037000l)};

        List l = Arrays.asList(da);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Writer w = TransitFactory.writer(TransitFactory.Format.MSGPACK, out);
        w.write(l);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        Reader r = TransitFactory.reader(TransitFactory.Format.MSGPACK, in);
        Object o = r.read();
    }

    public void testWriteReadSpecialNumbers() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Writer w = TransitFactory.writer(TransitFactory.Format.MSGPACK, out);
        w.write(Double.NaN);
        w.write(Float.NaN);
        w.write(Double.POSITIVE_INFINITY);
        w.write(Float.POSITIVE_INFINITY);
        w.write(Double.NEGATIVE_INFINITY);
        w.write(Float.NEGATIVE_INFINITY);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        Reader r = TransitFactory.reader(TransitFactory.Format.MSGPACK, in);
        assert((Double)r.read()).isNaN();
        assert((Double)r.read()).isNaN();
        assertEquals(Double.POSITIVE_INFINITY, (Double)r.read());
        assertEquals(Double.POSITIVE_INFINITY, (Double)r.read());
        assertEquals(Double.NEGATIVE_INFINITY, (Double)r.read());
        assertEquals(Double.NEGATIVE_INFINITY, (Double)r.read());
    }

}
