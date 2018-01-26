package misc;

import java.util.List;

public class KeyBuilder {
    public static byte[] buildKey(List<String> values){
        String tmp = "";
        for(String s: values){
            tmp+=s;
        }
        return tmp.getBytes();
    }
}
