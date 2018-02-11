package com.kinetica;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.Pointer;
import org.bytedeco.javacpp.annotation.Name;
import org.bytedeco.javacpp.annotation.NoException;
import org.bytedeco.javacpp.annotation.Platform;
import org.bytedeco.javacpp.annotation.StdString;

@Platform(include="com/kinetica/MemoryMappedFile.cpp")
final class MemoryMappedFile extends Pointer {
    static {
        Loader.load();
    }

    public MemoryMappedFile() {
        allocate();
    }

    private native void allocate();

    public native void map(@StdString String path, boolean writable, long size);
    public native void remap(long size);
    @NoException public native void unmap();

    @NoException public native long getSize();
    @NoException public native long getPos();
    public native void seek(long pos);

    public Map<String, byte[]> readBinaryMap(Map<String, byte[]> result) {
        long length = readLong();

        if (length > Integer.MAX_VALUE) {
            throw new RuntimeException("Invalid map length: " + length);
        }

        if (result == null) {
            result = new HashMap<>();
        }

        while (length > 0) {
            String key = readString();
            long valueLength = readLong();

            if (length > Integer.MAX_VALUE) {
                throw new RuntimeException("Invalid binary value length: " + length);
            }

            byte[] value = new byte[(int)valueLength];
            read(value, valueLength);
            result.put(key, value);
            length--;
        }

        return result;
    }

    @Name("read<int64_t>")
    public native long readLong();

    public String readString() {
        long length = readLong();

        if (length > Integer.MAX_VALUE) {
            throw new RuntimeException("Invalid string length: " + length);
        }

        byte[] buffer = new byte[(int)length];
        read(buffer, length);
        return new String(buffer, StandardCharsets.UTF_8);
    }

    public Map<String, String> readStringMap(Map<String, String> result) {
        long length = readLong();

        if (length > Integer.MAX_VALUE) {
            throw new RuntimeException("Invalid map length: " + length);
        }

        if (result == null) {
            result = new HashMap<>();
        }

        while (length > 0) {
            result.put(readString(), readString());
            length--;
        }

        return result;
    }

    public native void read(byte[] value, long length);

    @Name("read<int8_t>")
    @NoException public native byte readByte(long pos);

    @Name("read<double>")
    @NoException public native double readDouble(long pos);

    @Name("read<float>")
    @NoException public native float readFloat(long pos);

    @Name("read<int32_t>")
    @NoException public native int readInt(long pos);

    @Name("read<int64_t>")
    @NoException public native long readLong(long pos);

    @Name("read<int16_t>")
    @NoException public native short readShort(long pos);

    @NoException public native void read(long pos, byte[] value, long length);

    @NoException public native long readCharN(long pos, byte[] value, long size);

    public void writeBinaryMap(Map<String, byte[]> value) {
        writeLong(value.size());

        for (Map.Entry<String, byte[]> entry : value.entrySet()) {
            writeString(entry.getKey());
            write(entry.getValue(), entry.getValue().length);
        }
    }

    @Name("write<int8_t>")
    public native void writeByte(byte value);

    @Name("write<int64_t>")
    public native void writeLong(long value);

    public void writeString(String value) {
        byte[] buffer = value.getBytes(StandardCharsets.UTF_8);
        writeLong(buffer.length);
        write(buffer, buffer.length);
    }

    public void writeStringMap(Map<String, String> value) {
        writeLong(value.size());

        for (Map.Entry<String, String> entry : value.entrySet()) {
            writeString(entry.getKey());
            writeString(entry.getValue());
        }
    }

    public native void write(byte[] value, long length);

    @Name("write<int8_t>")
    @NoException public native void writeByte(long pos, byte value);

    @Name("write<double>")
    @NoException public native void writeDouble(long pos, double value);

    @Name("write<float>")
    @NoException public native void writeFloat(long pos, float value);

    @Name("write<int32_t>")
    @NoException public native void writeInt(long pos, int value);

    @Name("write<int64_t>")
    @NoException public native void writeLong(long pos, long value);

    @Name("write<int16_t>")
    @NoException public native void writeShort(long pos, short value);

    @NoException public native void write(long pos, byte[] value, long length);

    @NoException public native void writeCharN(long pos, byte[] value, long length, long size);

    public native void truncate();
    public native void lock(boolean exclusive);
    public native void unlock();
}