package cs.msu;

import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.io.Text;

public class SequenceFileUtils {
    public static String toSequenceString(Map<String, String> row, String[] fields) {
        final StringBuilder buffer = new StringBuilder();
        for (String field : fields) {
            String fieldValue = row.get(field);
            if (fieldValue != null) {
                buffer.append(fieldValue);
            }
            buffer.append('\01');
        }
        return buffer.toString();
    }

    public static Map<String, String> fromSequenceString(Text row, String[] fields) {
        final String[] tokens = row.toString().split("\01");
        final Map<String, String> map = new HashMap<>();

        for (int i = 0; i < tokens.length; i++) {
            map.put(fields[i], tokens[i]);
        }

        return map;
    }
}
