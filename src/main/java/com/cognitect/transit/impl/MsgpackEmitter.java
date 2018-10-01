// Copyright (c) Cognitect, Inc.
// All rights reserved.

package com.cognitect.transit.impl;

import com.cognitect.transit.WriteHandler;
import java.util.Base64;
import org.msgpack.packer.Packer;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

public class MsgpackEmitter extends AbstractEmitter {

    private final Packer gen;

    @Deprecated
    public MsgpackEmitter(Packer gen, WriteHandlerMap writeHandlerMap) {
        super(writeHandlerMap, null);
        this.gen = gen;
    }

    public MsgpackEmitter(Packer gen, WriteHandlerMap writeHandlerMap, WriteHandler defaultWriteHandler) {
        super(writeHandlerMap, defaultWriteHandler);
        this.gen = gen;
    }

    public MsgpackEmitter(Packer gen, WriteHandlerMap writeHandlerMap, WriteHandler defaultWriteHandler, Function<Object,Object> transform) {
        super(writeHandlerMap, defaultWriteHandler, transform);
        this.gen = gen;
    }

    @Override
    public void emit(Object o, boolean asMapKey, WriteCache cache) throws Exception {
        marshalTop(o, cache);
    }

    @Override
    public void emitNil(boolean asMapKey, WriteCache cache) throws Exception {
        this.gen.writeNil();
    }

@Override
    public void emitString(String prefix, String tag, String s, boolean asMapKey, WriteCache cache) throws Exception {
        String outString = cache.cacheWrite(Util.maybePrefix(prefix, tag, s), asMapKey);
        this.gen.write(outString);
    }

    @Override
    public void emitBoolean(Boolean b, boolean asMapKey, WriteCache cache) throws Exception {
        this.gen.write(b);
    }

    @Override
    public void emitBoolean(boolean b, boolean asMapKey, WriteCache cache) throws Exception {
        this.gen.write(b);
    }

    @Override
    public void emitInteger(Object o, boolean asMapKey, WriteCache cache) throws Exception {
        long i = Util.numberToPrimitiveLong(o);
        if ((i > Long.MAX_VALUE) || (i < Long.MIN_VALUE))
            this.emitString(Constants.ESC_STR, "i", o.toString(), asMapKey, cache);
        this.gen.write(i);
    }


    @Override
    public void emitInteger(long i, boolean asMapKey, WriteCache cache) throws Exception {
        if ((i > Long.MAX_VALUE) || (i < Long.MIN_VALUE))
            this.emitString(Constants.ESC_STR, "i", String.valueOf(i), asMapKey, cache);
        this.gen.write(i);
    }


    @Override
    public void emitDouble(Object d, boolean asMapKey, WriteCache cache) throws Exception {
        if (d instanceof Double)
            this.gen.write((Double) d);
        else if (d instanceof Float)
            this.gen.write((Float) d);
        else
            throw new Exception("Unknown floating point type: " + d.getClass());
    }

    @Override
    public void emitDouble(float d, boolean asMapKey, WriteCache cache) throws Exception {
        this.gen.write(d);
    }

    @Override
    public void emitDouble(double d, boolean asMapKey, WriteCache cache) throws Exception {
        this.gen.write(d);
    }

    @Override
    public void emitBinary(Object b, boolean asMapKey, WriteCache cache) throws Exception {
        byte[] encodedBytes = Base64.getEncoder().encode((byte[])b);
        emitString(Constants.ESC_STR, "b", new String(encodedBytes), asMapKey, cache);
    }

    @Override
    public void emitArrayStart(Long size) throws Exception {
        this.gen.writeArrayBegin(size.intValue());
    }

    @Override
    public void emitArrayEnd() throws Exception {
        this.gen.writeArrayEnd();
    }

    @Override
    public void emitMapStart(Long size) throws Exception {
        this.gen.writeMapBegin(size.intValue());
    }

    @Override
    public void emitMapEnd() throws Exception {
        this.gen.writeMapEnd();
    }

    @Override
    public void flushWriter() throws IOException {
        this.gen.flush();
    }

    @Override
    public boolean prefersStrings() {
        return false;
    }

    @Override
    protected void emitMap(Iterable<Map.Entry<Object, Object>> i, boolean ignored, WriteCache cache) throws Exception {
        emitMapStart(Util.mapSize(i));
        for (Map.Entry e : i) {
            marshal(e.getKey(), true, cache);
            marshal(e.getValue(), false, cache);
        }
        emitMapEnd();
    }
}
