package com.haoran;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

// 需求：计算一个字符串的长度
public class MyUDF extends GenericUDF {
    // 初始化方法，用于校验数据
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length !=1) {
            throw  new UDFArgumentLengthException("参数个数不为1");
        }
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    // 执行方法，处理数据
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object o = arguments[0].get();
        if(o==null){
            return 0;
        }
        return o.toString().length();
    }

    // 返回执行计划的顺序，返回""即可
    @Override
    public String getDisplayString(String[] children) {
        return "";
    }
}