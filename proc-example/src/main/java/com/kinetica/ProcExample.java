package com.kinetica;

public class ProcExample {
    public static void main(String[] args) {
        ProcData procData = ProcData.get();

        for (int i = 0; i < procData.getInputData().getTableCount(); i++) {
            ProcData.InputTable inputTable = procData.getInputData().getTable(i);
            ProcData.OutputTable outputTable = procData.getOutputData().getTable(i);
            outputTable.setSize(inputTable.getSize());

            for (int j = 0; j < inputTable.getColumnCount(); j++) {
                ProcData.InputColumn inputColumn = inputTable.getColumn(j);
                ProcData.OutputColumn outputColumn = outputTable.getColumn(j);

                for (long k = 0; k < inputTable.getSize(); k++) {
                    switch (inputColumn.getType()) {
                        case BYTES: outputColumn.appendVarBytes(inputColumn.getVarBytes(k)); break;
                        case CHAR1: outputColumn.appendChar(inputColumn.getChar(k)); break;
                        case CHAR2: outputColumn.appendChar(inputColumn.getChar(k)); break;
                        case CHAR4: outputColumn.appendChar(inputColumn.getChar(k)); break;
                        case CHAR8: outputColumn.appendChar(inputColumn.getChar(k)); break;
                        case CHAR16: outputColumn.appendChar(inputColumn.getChar(k)); break;
                        case CHAR32: outputColumn.appendChar(inputColumn.getChar(k)); break;
                        case CHAR64: outputColumn.appendChar(inputColumn.getChar(k)); break;
                        case CHAR128: outputColumn.appendChar(inputColumn.getChar(k)); break;
                        case CHAR256: outputColumn.appendChar(inputColumn.getChar(k)); break;
                        case DATE: outputColumn.appendInt(inputColumn.getInt(k)); break;
                        case DECIMAL: outputColumn.appendLong(inputColumn.getLong(k)); break;
                        case DOUBLE: outputColumn.appendDouble(inputColumn.getDouble(k)); break;
                        case FLOAT: outputColumn.appendFloat(inputColumn.getFloat(k)); break;
                        case INT: outputColumn.appendInt(inputColumn.getInt(k)); break;
                        case INT8: outputColumn.appendByte(inputColumn.getByte(k)); break;
                        case INT16: outputColumn.appendShort(inputColumn.getShort(k)); break;
                        case IPV4: outputColumn.appendInt(inputColumn.getInt(k)); break;
                        case LONG: outputColumn.appendLong(inputColumn.getLong(k)); break;
                        case STRING: outputColumn.appendVarString(inputColumn.getVarString(k)); break;
                        case TIME: outputColumn.appendInt(inputColumn.getInt(k)); break;
                        case TIMESTAMP: outputColumn.appendLong(inputColumn.getLong(k)); break;
                    }
                }
            }
        }

        procData.getResults().putAll(procData.getParams());
        procData.getBinResults().putAll(procData.getBinParams());
        procData.complete();
    }
}