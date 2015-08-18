package org.mitallast.queue.common.settings.loader;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonSettingsLoader implements SettingsLoader {

    @Override
    public Map<String, String> load(InputStream inputStream) throws IOException {
        try {
            JsonFactory factory = new JsonFactory();
            JsonParser parser = factory.createParser(inputStream);
            return load(parser);
        } finally {
            inputStream.close();
        }
    }

    public Map<String, String> load(JsonParser parser) throws IOException {
        StringBuilder sb = new StringBuilder();
        Map<String, String> settings = new HashMap<>();
        List<String> path = new ArrayList<>();
        JsonToken token = parser.nextToken();
        if (token == null) {
            return settings;
        }
        if (token != JsonToken.START_OBJECT) {
            throw new IllegalStateException("malformed, expected settings to start with 'object', instead was [" + token + "]");
        }
        serializeObject(settings, sb, path, parser, null);
        return settings;
    }

    private void serializeObject(Map<String, String> settings, StringBuilder sb, List<String> path, JsonParser parser, String objFieldName) throws IOException {
        if (objFieldName != null) {
            path.add(objFieldName);
        }

        String currentFieldName = null;
        JsonToken token;
        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.START_OBJECT) {
                serializeObject(settings, sb, path, parser, currentFieldName);
            } else if (token == JsonToken.START_ARRAY) {
                serializeArray(settings, sb, path, parser, currentFieldName);
            } else if (token == JsonToken.FIELD_NAME) {
                currentFieldName = parser.getCurrentName();
            } else if (token == JsonToken.VALUE_NULL) {
                // ignore this
            } else {
                serializeValue(settings, sb, path, parser, currentFieldName);
            }
        }

        if (objFieldName != null) {
            path.remove(path.size() - 1);
        }
    }

    private void serializeArray(Map<String, String> settings, StringBuilder sb, List<String> path, JsonParser parser, String fieldName) throws IOException {
        JsonToken token;
        int counter = 0;
        while ((token = parser.nextToken()) != JsonToken.END_ARRAY) {
            if (token == JsonToken.START_OBJECT) {
                serializeObject(settings, sb, path, parser, fieldName + '.' + (counter++));
            } else if (token == JsonToken.START_ARRAY) {
                serializeArray(settings, sb, path, parser, fieldName + '.' + (counter++));
            } else if (token == JsonToken.FIELD_NAME) {
                fieldName = parser.getCurrentName();
            } else if (token == JsonToken.VALUE_NULL) {
                // ignore
            } else {
                serializeValue(settings, sb, path, parser, fieldName + '.' + (counter++));
            }
        }
    }

    private void serializeValue(Map<String, String> settings, StringBuilder sb, List<String> path, JsonParser parser, String fieldName) throws IOException {
        sb.setLength(0);
        for (String pathEle : path) {
            sb.append(pathEle).append('.');
        }
        sb.append(fieldName);
        settings.put(sb.toString(), parser.getText());
    }
}
