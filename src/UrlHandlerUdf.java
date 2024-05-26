package cn.wolfcode.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.nio.charset.StandardCharsets;

// 对于url的参数进行处理
public class UrlHandlerUdf  extends GenericUDF {
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        // 参数长度判断
        if(objectInspectors.length != 1){
            throw new UDFArgumentLengthException("传入的数据参数的长度不正确!");
        }
        // 判断输入参数的类型
        if(!objectInspectors[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw new UDFArgumentTypeException(0,"输入的参数类型不正确!!!");
        }
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if(deferredObjects[0].get() == null){
            return "" ;
        }
        String data = deferredObjects[0].get().toString();
        int index = data.indexOf("?");
        if(index > 0 ){
            data = data.substring(0,index);
        }

        if (data.startsWith("https://")){
            data=data.replaceFirst("https://","http://");
        }

        return new Text(data.getBytes(StandardCharsets.UTF_8));

    }

    @Override
    public String getDisplayString(String[] strings) {
        return "";
    }

    public static void main(String[] args) {
        String data = "https://www.baidu.com/wd?abc=123";

        int index = data.indexOf("?");
        if(index > 0 ){
            data = data.substring(0,index);
        }

        if (data.startsWith("https://")){
            data=data.replaceFirst("https://","http://");
        }

        System.out.println(data);
    }
}
