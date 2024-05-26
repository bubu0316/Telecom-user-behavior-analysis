package cn.wolfcode.udf;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import redis.clients.jedis.Jedis;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Ip2Loc extends GenericUDF {
    public static List<Map<String,String>> mapList = new ArrayList<>();
    static {
        String host = "192.168.66.100";
        int port = 6379;
        Jedis jedis = new Jedis(host, port);
        Set<String> areas = jedis.smembers("areas");
        for (String area : areas) {
            JSONObject jsonObject = JSON.parseObject(area);
            Map<String,String> map = new HashMap<>();
            map.put("city",jsonObject.getString("city"));
            map.put("province",jsonObject.getString("province"));
            mapList.add(map);
        }
        // 把map数据写入到文件
    }
    // 初始化参数判断
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

    // 返回一个字符串 广东省|广州市
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        if(deferredObjects[0].get() == null){
            return "" ;
        }
//        DefaultHttpClient defaultHttpClient = new DefaultHttpClient();
//        HttpGet httpGet = new HttpGet("https://apis.map.qq.com/ws/location/v1/ip?ip="+deferredObjects[0].get()+"&key=FFFBZ-GUHEQ-GEA5Z-G5HJU-MRFY3-JEFIG");
//        try {
//            HttpResponse response = defaultHttpClient.execute(httpGet);
//            int statusCode = response.getStatusLine().getStatusCode();
//            if (statusCode == 200) {
//                HttpEntity entity = response.getEntity();
//                String jsonResult = EntityUtils.toString(entity);
//                JSONObject jsonObject = JSON.parseObject(jsonResult);
//                String city = jsonObject.getJSONObject("result").getJSONObject("ad_info").getString("city");
//                String province = jsonObject.getJSONObject("result").getJSONObject("ad_info").getString("province");
//                return city+"|"+province;
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        // 1.创建Jedis连接
//        String host = "192.168.46.1";
//        int port = 6379;
//        Jedis jedis = new Jedis(host, port);
//        // 2.检测连通性
//        String result = jedis.srandmember("areas");
//        JSONObject jsonObject = JSON.parseObject(result);
//        // 3.关闭Jedis连接
//        jedis.close();
        int index = (int) (Math.random() * mapList.size());
        Text new_str = new Text((mapList.get(index).get("city")+"_"+(mapList.get(index).get("province"))).getBytes(StandardCharsets.UTF_8));
        return new_str;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "";
    }

    public static void main2(String[] args) throws Exception {
//        DefaultHttpClient defaultHttpClient = new DefaultHttpClient();
//        HttpGet httpGet = new HttpGet("https://apis.map.qq.com/ws/location/v1/ip?ip=182.92.230.125&key=FFFBZ-GUHEQ-GEA5Z-G5HJU-MRFY3-JEFIG");
//        HttpResponse response = defaultHttpClient.execute(httpGet);
//        int statusCode = response.getStatusLine().getStatusCode();
//        if (statusCode == 200) {
//            HttpEntity entity = response.getEntity();
//            String jsonResult = EntityUtils.toString(entity);
//            JSONObject jsonObject = JSON.parseObject(jsonResult);
//            System.out.println("statusCode--请求成功json--->:"+jsonResult);
//            System.out.println(jsonObject.getJSONObject("result").getJSONObject("ad_info").getString("city"));
//            System.out.println(jsonObject.getJSONObject("result").getJSONObject("ad_info").getString("province"));
//        }else {
//            System.out.println("statusCode--请求失败:"+statusCode);
//        }
    }

    public static void main(String[] args) {
//        String host = "192.168.46.1";
//        int port = 6379;
//        Jedis jedis = new Jedis(host, port);
//        String result = jedis.srandmember("areas");
//        JSONObject jsonObject = JSON.parseObject(result);
//        System.out.println(jsonObject.getString("city")+"|"+jsonObject.getString("province"));
        int index = (int) (Math.random() * mapList.size());
        System.out.println(mapList.get(index).get("city") + "_" + mapList.get(index).get("province"));
    }
}
