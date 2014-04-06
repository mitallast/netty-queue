package org.mitallast.queue.common.settings.loader;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertiesSettingsLoader implements SettingsLoader {

    @Override
    public Map<String, String> load(InputStream inputStream) throws IOException {
        try {
            Properties props = new Properties();
            props.load(inputStream);
            Map<String, String> result = new HashMap<>(props.size());
            for (Map.Entry entry : props.entrySet()) {
                result.put((String) entry.getKey(), (String) entry.getValue());
            }
            return result;
        } finally {
            inputStream.close();
        }
    }
}
