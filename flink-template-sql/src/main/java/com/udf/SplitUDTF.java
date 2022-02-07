package com.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW<user STRING>"))
public class SplitUDTF extends TableFunction<Row> {

    public void eval(String str, String seq) {

        String[] splitArr = str.split(seq);
        //遍历单词写出
        for (String user : splitArr) {
            collect(Row.of(user));
        }
    }
}
