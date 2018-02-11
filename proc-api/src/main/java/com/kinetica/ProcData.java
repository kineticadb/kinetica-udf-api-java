package com.kinetica;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Formatter;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class ProcData {
    public static enum ColumnType {
        BYTES    (0x0000002),
        CHAR1    (0x0080000),
        CHAR2    (0x0100000),
        CHAR4    (0x0001000),
        CHAR8    (0x0002000),
        CHAR16   (0x0004000),
        CHAR32   (0x0200000),
        CHAR64   (0x0400000),
        CHAR128  (0x0800000),
        CHAR256  (0x1000000),
        DATE     (0x2000000),
        DATETIME (0x0000200),
        DECIMAL  (0x8000000),
        DOUBLE   (0x0000010),
        FLOAT    (0x0000020),
        INT      (0x0000040),
        INT8     (0x0020000),
        INT16    (0x0040000),
        IPV4     (0x0008000),
        LONG     (0x0000080),
        STRING   (0x0000001),
        TIME     (0x4000000),
        TIMESTAMP(0x0010000);

        private final int value;
        private int size;

        public static ColumnType fromInt(int value) {
            ColumnType[] types = ColumnType.values();

            for (ColumnType type : types) {
                if (type.value == value) {
                    return type;
                }
            }

            throw new RuntimeException("Unknown data type: " + value);
        }

        private ColumnType(int value) {
            this.value = value;
        }

        static {
            for (ColumnType type : ColumnType.values()) {
                switch (type) {
                    case BYTES:     type.size = 8; break;
                    case CHAR1:     type.size = 1; break;
                    case CHAR2:     type.size = 2; break;
                    case CHAR4:     type.size = 4; break;
                    case CHAR8:     type.size = 8; break;
                    case CHAR16:    type.size = 16; break;
                    case CHAR32:    type.size = 32; break;
                    case CHAR64:    type.size = 64; break;
                    case CHAR128:   type.size = 128; break;
                    case CHAR256:   type.size = 256; break;
                    case DATE:      type.size = 4; break;
                    case DATETIME:  type.size = 8; break;
                    case DECIMAL:   type.size = 8; break;
                    case DOUBLE:    type.size = 8; break;
                    case FLOAT:     type.size = 4; break;
                    case INT:       type.size = 4; break;
                    case INT8:      type.size = 1; break;
                    case INT16:     type.size = 2; break;
                    case IPV4:      type.size = 4; break;
                    case LONG:      type.size = 8; break;
                    case STRING:    type.size = 8; break;
                    case TIME:      type.size = 4; break;
                    case TIMESTAMP: type.size = 8; break;
                    default: throw new IllegalStateException("Invalid data type");
                }
            }
        }

        public int getSize() {
            return size;
        }
    }

    public static class Column {
        private static final SimpleDateFormat DATE_FORMAT;
        private static final SimpleDateFormat DATETIME_FORMAT;
        private static final SimpleDateFormat TIME_FORMAT;
        protected static final TimeZone UTC;

        static {
            UTC = TimeZone.getTimeZone("UTC");
            GregorianCalendar calendar = new GregorianCalendar(UTC);
            calendar.setGregorianChange(new Date(Long.MIN_VALUE));
            DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
            DATE_FORMAT.setCalendar(calendar);
            DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            DATETIME_FORMAT.setCalendar(calendar);
            TIME_FORMAT = new SimpleDateFormat("HH:mm:ss.SSS");
            TIME_FORMAT.setCalendar(calendar);
        }

        protected final String name;
        protected final ColumnType type;
        protected final boolean isNullable;
        protected long size;
        protected final MemoryMappedFile data;
        protected final MemoryMappedFile nulls;
        protected final MemoryMappedFile varData;

        private Column(MemoryMappedFile file, boolean writable) {
            name = file.readString();
            type = ColumnType.fromInt((int)file.readLong());
            String dataPath = file.readString();
            data = new MemoryMappedFile();

            if (!dataPath.isEmpty()) {
                data.map(dataPath, writable, -1);
                size = data.getSize() / type.getSize();
            } else {
                size = 0;
            }

            String nullsPath = file.readString();
            nulls = new MemoryMappedFile();

            if (!nullsPath.isEmpty()) {
                nulls.map(nullsPath, writable, -1);
                isNullable = true;
            } else {
                isNullable = false;
            }

            String varDataPath = file.readString();
            varData = new MemoryMappedFile();

            if (!varDataPath.isEmpty()) {
                varData.map(varDataPath, writable, -1);
            }
        }

        public String getName() {
            return name;
        }

        public ColumnType getType() {
            return type;
        }

        public boolean isNullable() {
            return isNullable;
        }

        public long getSize() {
            return size;
        }

        public boolean isNull(long index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException("Invalid index specified");
            }

            if (!isNullable) {
                return false;
            } else {
                return nulls.readByte(index) != 0;
            }
        }

        public BigDecimal getBigDecimal(long index) {
            if (type != ColumnType.DECIMAL) {
                throw new IllegalStateException("Incompatible data type");
            }

            if (isNull(index)) {
                return null;
            } else {
                return new BigDecimal(data.readLong(index * 8)).movePointLeft(4);
            }
        }

        public Byte getByte(long index) {
            if (type != ColumnType.INT8) {
                throw new IllegalStateException("Incompatible data type");
            }

            if (isNull(index)) {
                return null;
            } else {
                return data.readByte(index);
            }
        }

        public Calendar getCalendar(long index) {
            switch (type) {
                case DATE:
                    if (isNull(index)) {
                        return null;
                    } else {
                        int value = data.readInt(index * 4);
                        GregorianCalendar result = new GregorianCalendar(UTC);
                        result.setGregorianChange(new Date(Long.MIN_VALUE));
                        result.clear();
                        result.set(1900 + (value >> 21),
                                ((value >> 17) & 0b1111) - 1,
                                (value >> 12) & 0b11111);
                        return result;
                    }

                case DATETIME:
                    if (isNull(index)) {
                        return null;
                    } else {
                        long value = data.readLong(index * 8);
                        GregorianCalendar result = new GregorianCalendar(UTC);
                        result.setGregorianChange(new Date(Long.MIN_VALUE));
                        result.clear();
                        result.set(1900 + (int)(value >> 53),
                                (int)((value >> 49) & 0b1111) - 1,
                                (int)((value >> 44) & 0b11111),
                                (int)((value >> 39) & 0b11111),
                                (int)((value >> 33) & 0b111111),
                                (int)((value >> 27) & 0b111111));
                        result.set(Calendar.MILLISECOND, (int)((value >> 17) & 0b1111111111));
                        return result;
                    }

                case TIME:
                    if (isNull(index)) {
                        return null;
                    } else {
                        int value = data.readInt(index * 4);
                        GregorianCalendar result = new GregorianCalendar(UTC);
                        result.setGregorianChange(new Date(Long.MIN_VALUE));
                        result.clear();
                        result.set(Calendar.HOUR_OF_DAY, (value >> 26));
                        result.set(Calendar.MINUTE, (value >> 20) & 0b111111);
                        result.set(Calendar.SECOND, (value >> 14) & 0b111111);
                        result.set(Calendar.MILLISECOND, (value >> 4) & 0b1111111111);
                        return result;
                    }

                default:
                    throw new IllegalStateException("Incompatible data type");
            }
        }

        public String getChar(long index) {
            switch (type) {
                case CHAR1:
                case CHAR2:
                case CHAR4:
                case CHAR8:
                case CHAR16:
                case CHAR32:
                case CHAR64:
                case CHAR128:
                case CHAR256:
                    break;

                default:
                    throw new IllegalStateException("Incompatible data type");
            }

            if (isNull(index)) {
                return null;
            } else {
                int valueSize = type.getSize();
                byte[] buffer = new byte[valueSize];
                long length = data.readCharN(index * valueSize, buffer, valueSize);
                return new String(buffer, 0, (int)length, StandardCharsets.UTF_8);
            }
        }

        public Double getDouble(long index) {
            if (type != ColumnType.DOUBLE) {
                throw new IllegalStateException("Incompatible data type");
            }

            if (isNull(index)) {
                return null;
            } else {
                return data.readDouble(index * 8);
            }
        }

        public Float getFloat(long index) {
            if (type != ColumnType.FLOAT) {
                throw new IllegalStateException("Incompatible data type");
            }

            if (isNull(index)) {
                return null;
            } else {
                return data.readFloat(index * 4);
            }
        }

        public Inet4Address getInet4Address(long index) {
            if (type != ColumnType.IPV4) {
                throw new IllegalStateException("Incompatible data type");
            }

            if (isNull(index)) {
                return null;
            } else {
                int value = data.readInt(index * 4);
                byte[] buffer = new byte[4];
                buffer[0] = (byte)((value & 0xFF000000) >> 24);
                buffer[1] = (byte)((value & 0x00FF0000) >> 16);
                buffer[2] = (byte)((value & 0x0000FF00) >> 8);
                buffer[3] = (byte)((value & 0x000000FF));

                try {
                    return (Inet4Address)InetAddress.getByAddress(buffer);
                } catch (UnknownHostException ex) {
                    throw new IllegalStateException(ex.getMessage());
                }
            }
        }

        public Integer getInt(long index) {
            switch (type) {
                case DATE:
                case INT:
                case IPV4:
                case TIME:
                    break;

                default:
                    throw new IllegalStateException("Incompatible data type");
            }

            if (isNull(index)) {
                return null;
            } else {
                return data.readInt(index * 4);
            }
        }

        public Long getLong(long index) {
            switch (type) {
                case DATETIME:
                case DECIMAL:
                case LONG:
                case TIMESTAMP:
                    break;

                default:
                    throw new IllegalStateException("Incompatible data type");
            }

            if (isNull(index)) {
                return null;
            } else {
                return data.readLong(index * 8);
            }
        }

        public Short getShort(long index) {
            if (type != ColumnType.INT16) {
                throw new IllegalStateException("Incompatible data type");
            }

            if (isNull(index)) {
                return null;
            } else {
                return data.readShort(index * 2);
            }
        }

        public byte[] getVarBytes(long index) {
            if (type != ColumnType.BYTES) {
                throw new IllegalStateException("Incompatible data type");
            }

            if (isNull(index)) {
                return null;
            } else {
                long position = data.readLong(index * 8);
                long valueSize;

                if (index < size - 1) {
                    valueSize = data.readLong((index + 1) * 8) - position;
                } else {
                    valueSize = varData.getSize() - position;
                }

                byte[] buffer = new byte[(int)valueSize];
                varData.read(position, buffer, (int)valueSize);
                return buffer;
            }
        }

        public String getVarString(long index) {
            if (type != ColumnType.STRING) {
                throw new IllegalStateException("Incompatible data type");
            }

            if (isNull(index)) {
                return null;
            } else {
                long position = data.readLong(index * 8);
                long valueSize;

                if (index < size - 1) {
                    valueSize = data.readLong((index + 1) * 8) - position - 1;
                } else {
                    valueSize = varData.getSize() - position - 1;
                }

                byte[] buffer = new byte[(int)valueSize];
                varData.read(position, buffer, (int)valueSize);
                return new String(buffer, StandardCharsets.UTF_8);
            }
        }

        public String toString(long index) {
            if (isNull(index)) {
                return "";
            } else {
                switch (type) {
                    case BYTES: {
                        Formatter formatter = new Formatter();

                        for (byte b : getVarBytes(index)) {
                            formatter.format("%02x", b);
                        }

                        return formatter.toString();
                    }

                    case CHAR1: return getChar(index);
                    case CHAR2: return getChar(index);
                    case CHAR4: return getChar(index);
                    case CHAR8: return getChar(index);
                    case CHAR16: return getChar(index);
                    case CHAR32: return getChar(index);
                    case CHAR64: return getChar(index);
                    case CHAR128: return getChar(index);
                    case CHAR256: return getChar(index);
                    case DATE: return DATE_FORMAT.format(getCalendar(index).getTime());
                    case DATETIME: return DATETIME_FORMAT.format(getCalendar(index).getTime());
                    case DECIMAL: return getBigDecimal(index).toString();
                    case DOUBLE: return getDouble(index).toString();
                    case FLOAT: return getFloat(index).toString();
                    case INT: return getInt(index).toString();
                    case INT8: return getByte(index).toString();
                    case INT16: return getShort(index).toString();
                    case IPV4: return getInet4Address(index).getHostAddress();
                    case LONG: return getLong(index).toString();
                    case STRING: return getVarString(index);
                    case TIME: return TIME_FORMAT.format(getCalendar(index).getTime());
                    case TIMESTAMP: return getLong(index).toString();
                    default: throw new IllegalStateException("Invalid data type");
                }
            }
        }
    }

    public static final class InputColumn extends Column {
        private InputColumn(MemoryMappedFile file) {
            super(file, false);
        }
    }

    public static final class OutputColumn extends Column {
        private long pos;

        private OutputColumn(MemoryMappedFile file) {
            super(file, true);
        }

        public void setNull(long index) {
            if (!isNullable) {
                throw new IllegalStateException("Column " + name + " is not nullable");
            }

            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException("Invalid index specified");
            }

            nulls.writeByte(index, (byte)1);
        }

        private boolean handleNull(long index, Object value) {
            if (value == null) {
                setNull(index);
                return true;
            } else {
                if (index < 0 || index >= size) {
                    throw new IndexOutOfBoundsException("Invalid index specified");
                }

                if (isNullable) {
                    nulls.writeByte(index, (byte)0);
                }

                return false;
            }
        }

        public void setBigDecimal(long index, BigDecimal value) {
            if (type != ColumnType.DECIMAL) {
                throw new IllegalStateException("Incompatible data type");
            }

            if (!handleNull(index, value)) {
                data.writeLong(index * 8, value.movePointRight(4).longValueExact());
            }
        }

        public void setByte(long index, Byte value) {
            if (type != ColumnType.INT8) {
                throw new IllegalStateException("Incompatible data type");
            }

            if (!handleNull(index, value)) {
                data.writeByte(index, value);
            }
        }

        public void setCalendar(long index, Calendar value) {
            switch (type) {
                case DATE:
                    if (!handleNull(index, value)) {
                        GregorianCalendar newValue = new GregorianCalendar(UTC);
                        newValue.setGregorianChange(new Date(Long.MIN_VALUE));
                        newValue.setTimeInMillis(value.getTimeInMillis());
                        data.writeInt(index * 4,
                                ((newValue.get(Calendar.YEAR) - 1900) << 21)
                                | ((newValue.get(Calendar.MONTH) + 1) << 17)
                                | (newValue.get(Calendar.DAY_OF_MONTH) << 12));
                    }

                    break;

                case DATETIME:
                    if (!handleNull(index, value)) {
                        GregorianCalendar newValue = new GregorianCalendar(UTC);
                        newValue.setGregorianChange(new Date(Long.MIN_VALUE));
                        newValue.setTimeInMillis(value.getTimeInMillis());
                        data.writeLong(index * 8,
                                (((long)newValue.get(Calendar.YEAR) - 1900) << 53)
                                | (((long)newValue.get(Calendar.MONTH) + 1) << 49)
                                | ((long)newValue.get(Calendar.DAY_OF_MONTH) << 44)
                                | ((long)newValue.get(Calendar.HOUR_OF_DAY) << 39)
                                | ((long)newValue.get(Calendar.MINUTE) << 33)
                                | ((long)newValue.get(Calendar.SECOND) << 27)
                                | ((long)newValue.get(Calendar.MILLISECOND) << 17));
                        break;
                    }

                case TIME:
                    if (!handleNull(index, value)) {
                        GregorianCalendar newValue = new GregorianCalendar(UTC);
                        newValue.setGregorianChange(new Date(Long.MIN_VALUE));
                        newValue.setTimeInMillis(value.getTimeInMillis());
                        data.writeInt(index * 4,
                                (newValue.get(Calendar.HOUR_OF_DAY) << 26)
                                | (newValue.get(Calendar.MINUTE) << 20)
                                | (newValue.get(Calendar.SECOND) << 14)
                                | (newValue.get(Calendar.MILLISECOND) << 4));
                    }

                    break;

                default:
                    throw new IllegalStateException("Incompatible data type");
            }
        }

        public void setChar(long index, String value) {
            switch (type) {
                case CHAR1:
                case CHAR2:
                case CHAR4:
                case CHAR8:
                case CHAR16:
                case CHAR32:
                case CHAR64:
                case CHAR128:
                case CHAR256:
                    break;

                default:
                    throw new IllegalStateException("Incompatible data type");
            }

            if (!handleNull(index, value)) {
                int valueSize = type.getSize();
                byte[] buffer = value.getBytes(StandardCharsets.UTF_8);
                data.writeCharN(index * valueSize, buffer, buffer.length, valueSize);
            }
        }

        public void setDouble(long index, Double value) {
            if (type != ColumnType.DOUBLE) {
                throw new IllegalStateException("Incompatible data type");
            }

            if (!handleNull(index, value)) {
                data.writeDouble(index * 8, value);
            }
        }

        public void setFloat(long index, Float value) {
            if (type != ColumnType.FLOAT) {
                throw new IllegalStateException("Incompatible data type");
            }

            if (!handleNull(index, value)) {
                data.writeFloat(index * 4, value);
            }
        }

        public void setInet4Address(long index, Inet4Address value) {
            if (type != ColumnType.IPV4) {
                throw new IllegalStateException("Incompatible data type");
            }

            if (!handleNull(index, value)) {
                byte[] buffer = value.getAddress();
                data.writeInt(index * 4, buffer[0] << 24 | buffer[1] << 16 | buffer[2] << 8 | buffer[3]);
            }
        }

        public void setInt(long index, Integer value) {
            switch (type) {
                case DATE:
                case INT:
                case IPV4:
                case TIME:
                    break;

                default:
                    throw new IllegalStateException("Incompatible data type");
            }

            if (!handleNull(index, value)) {
                data.writeInt(index * 4, value);
            }
        }

        public void setLong(long index, Long value) {
            switch (type) {
                case DATETIME:
                case DECIMAL:
                case LONG:
                case TIMESTAMP:
                    break;

                default:
                    throw new IllegalStateException("Incompatible data type");
            }

            if (!handleNull(index, value)) {
                data.writeLong(index * 8, value);
            }
        }

        public void setShort(long index, Short value) {
            if (type != ColumnType.INT16) {
                throw new IllegalStateException("Incompatible data type");
            }

            if (!handleNull(index, value)) {
                data.writeShort(index * 2, value);
            }
        }

        public long appendBigDecimal(BigDecimal value) {
            long index = pos;
            setBigDecimal(pos, value);
            pos++;
            return index;
        }

        public long appendByte(Byte value) {
            long index = pos;
            setByte(pos, value);
            pos++;
            return index;
        }

        public long appendCalendar(Calendar value) {
            long index = pos;
            setCalendar(pos, value);
            pos++;
            return index;
        }

        public long appendChar(String value) {
            long index = pos;
            setChar(pos, value);
            pos++;
            return index;
        }

        public long appendDouble(Double value) {
            long index = pos;
            setDouble(pos, value);
            pos++;
            return index;
        }

        public long appendFloat(Float value) {
            long index = pos;
            setFloat(pos, value);
            pos++;
            return index;
        }

        public long appendInet4Address(Inet4Address value) {
            long index = pos;
            setInet4Address(pos, value);
            pos++;
            return index;
        }

        public long appendInt(Integer value) {
            long index = pos;
            setInt(pos, value);
            pos++;
            return index;
        }

        public long appendLong(Long value) {
            long index = pos;
            setLong(pos, value);
            pos++;
            return index;
        }

        public long appendShort(Short value) {
            long index = pos;
            setShort(pos, value);
            pos++;
            return index;
        }

        public long appendVarBytes(byte[] value) {
            if (type != ColumnType.BYTES) {
                throw new IllegalStateException("Incompatible data type");
            }

            long index = pos;
            data.writeLong(index * 8, varData.getPos());

            if (!handleNull(index, value)) {
                varData.write(value, value.length);
            }

            pos++;
            return index;
        }

        public long appendVarString(String value) {
            if (type != ColumnType.STRING) {
                throw new IllegalStateException("Incompatible data type");
            }

            long index = pos;
            data.writeLong(index * 8, varData.getPos());

            if (!handleNull(index, value)) {
                byte[] buffer = value.getBytes(StandardCharsets.UTF_8);
                varData.write(buffer, buffer.length);
                varData.writeByte((byte)0);
            }

            pos++;
            return index;
        }

        private void complete() {
            if (type == ColumnType.BYTES || type == ColumnType.STRING) {
                varData.truncate();
            }
        }

        private void reserve(long size) {
            data.remap(size * type.getSize());

            if (isNullable()) {
                nulls.remap(size);
            }

            this.size = size;
        }
    }

    private static interface ColumnCreator {
        Column create(MemoryMappedFile file);
    }

    public static abstract class Table<T> {
        protected final String name;
        protected long size;
        protected List<Column> columns;
        protected Map<String, Integer> columnMap;

        private Table(MemoryMappedFile file, ColumnCreator columnCreator) {
            name = file.readString();
            long columnCount = file.readLong();

            if (columnCount < 0 || columnCount > Integer.MAX_VALUE) {
                throw new RuntimeException("Invalid column count: " + Long.toString(columnCount));
            }

            size = 0;
            columns = new ArrayList<>((int)columnCount);
            columnMap = new HashMap<>((int)columnCount);

            for (int i = 0; i < columnCount; i++) {
                Column column = columnCreator.create(file);
                columns.add(column);
                columnMap.put(column.getName(), i);

                if (i == 0 || column.getSize() < size) {
                    size = column.getSize();
                }
            }

            this.columns = Collections.unmodifiableList(columns);
        }

        public String getName() {
            return name;
        }

        public long getSize() {
            return size;
        }

        @SuppressWarnings("unchecked")
        public List<T> getColumns() {
            return (List<T>)columns;
        }

        public int getColumnCount() {
            return columns.size();
        }

        @SuppressWarnings("unchecked")
        public T getColumn(int index) {
            return (T)columns.get(index);
        }

        @SuppressWarnings("unchecked")
        public T getColumn(String name) {
            Integer index = columnMap.get(name);
            return index == null ? null : (T)columns.get(index);
        }

        public int getColumnIndex(String name) {
            Integer result = columnMap.get(name);
            return result == null ? -1 : result;
        }
    }

    public static final class InputTable extends Table<InputColumn> {
        private InputTable(MemoryMappedFile file) {
            super(file, new ColumnCreator() {
                @Override
                public Column create(MemoryMappedFile file) {
                    return new InputColumn(file);
                }
            });
        }
    }

    public static final class OutputTable extends Table<OutputColumn> {
        private OutputTable(MemoryMappedFile file) {
            super(file, new ColumnCreator() {
                @Override
                public Column create(MemoryMappedFile file) {
                    return new OutputColumn(file);
                }
            });
        }

        public void setSize(long size) {
            if (size < 0) {
                throw new IllegalArgumentException("Invalid size specified");
            }

            for (Column column : columns) {
                ((OutputColumn)column).reserve(size);
            }

            this.size = size;
        }

        private void complete() {
            for (Column column : columns) {
                ((OutputColumn)column).complete();
            }
        }
    }

    private static interface TableCreator {
        Table<?> create(MemoryMappedFile file);
    }

    public static abstract class DataSet<T> {
        protected List<Table<?>> tables;
        protected Map<String, Integer> tableMap;

        private DataSet(MemoryMappedFile file, TableCreator tableCreator) {
            long tableCount = file.readLong();

            if (tableCount < 0 || tableCount > Integer.MAX_VALUE) {
                throw new RuntimeException("Invalid table count: " + tableCount);
            }

            tables = new ArrayList<>((int)tableCount);
            tableMap = new HashMap<>((int)tableCount);

            for (int i = 0; i < tableCount; i++) {
                Table<?> table = tableCreator.create(file);
                tables.add(table);
                tableMap.put(table.getName(), i);
            }

            tables = Collections.unmodifiableList(tables);
        }

        @SuppressWarnings("unchecked")
        public List<T> getTables() {
            return (List<T>)tables;
        }

        public int getTableCount() {
            return tables.size();
        }

        @SuppressWarnings("unchecked")
        public T getTable(int index) {
            return (T)tables.get(index);
        }

        @SuppressWarnings("unchecked")
        public T getTable(String name) {
            Integer index = tableMap.get(name);
            return index == null ? null : (T)tables.get(index);
        }

        public int getTableIndex(String name) {
            Integer result = tableMap.get(name);
            return result == null ? -1 : result;
        }
    }

    public static class InputDataSet extends DataSet<InputTable> {
        private InputDataSet(MemoryMappedFile file) {
            super(file, new TableCreator() {
                @Override
                public Table<?> create(MemoryMappedFile file) {
                    return new InputTable(file);
                }
            });
        }
    }

    public static class OutputDataSet extends DataSet<OutputTable> {
        private OutputDataSet(MemoryMappedFile file) {
            super(file, new TableCreator() {
                @Override
                public Table<?> create(MemoryMappedFile file) {
                    return new OutputTable(file);
                }
            });
        }

        private void complete() {
            for (Table<?> table : tables) {
                ((OutputTable)table).complete();
            }
        }
    }

    private static ProcData theProcData;

    public static ProcData get() {
        if (theProcData == null) {
            theProcData = new ProcData();
        }

        return theProcData;
    }

    private Map<String, String> requestInfo;
    private Map<String, String> params;
    private Map<String, byte[]> binParams;
    private InputDataSet inputData;
    private String outputControlFileName;
    private Map<String, String> results;
    private Map<String, byte[]> binResults;
    private OutputDataSet outputData;
    private String status;
    private MemoryMappedFile statusFile;

    private ProcData() {
        String controlFileName = System.getenv("KINETICA_PCF");

        if (controlFileName == null) {
            throw new RuntimeException("No control file specified");
        }

        MemoryMappedFile controlFile = new MemoryMappedFile();
        controlFile.map(controlFileName, false, -1);

        long version = controlFile.readLong();

        if (version != 1 && version != 2) {
            throw new RuntimeException("Unrecognized control file version: " + Long.toString(version));
        }

        requestInfo = controlFile.readStringMap(null);
        controlFile.readStringMap(requestInfo);
        requestInfo = Collections.unmodifiableMap(requestInfo);
        params = Collections.unmodifiableMap(controlFile.readStringMap(null));
        binParams = Collections.unmodifiableMap(controlFile.readBinaryMap(null));
        inputData = new InputDataSet(controlFile);
        outputData = new OutputDataSet(controlFile);
        outputControlFileName = controlFile.readString();

        if (version == 2) {
            statusFile = new MemoryMappedFile();
            statusFile.map(controlFile.readString(), true, -1);
        }

        results = new HashMap<>();
        binResults = new HashMap<>();
    }

    public Map<String, String> getRequestInfo() {
        return requestInfo;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public Map<String, byte[]> getBinParams() {
        return binParams;
    }

    public InputDataSet getInputData() {
        return inputData;
    }

    public Map<String, String> getResults() {
        return results;
    }

    public Map<String, byte[]> getBinResults() {
        return binResults;
    }

    public OutputDataSet getOutputData() {
        return outputData;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String value) {
        status = value;

        if (statusFile != null) {
            statusFile.lock(true);

            try {
                statusFile.seek(0);
                statusFile.writeString(value);
            } finally {
                statusFile.unlock();
            }
        }
    }

    public void complete() {
        outputData.complete();
        MemoryMappedFile controlFile = new MemoryMappedFile();
        controlFile.map(outputControlFileName, true, -1);
        controlFile.writeLong(1);
        controlFile.writeStringMap(results);
        controlFile.writeBinaryMap(binResults);
    }
}