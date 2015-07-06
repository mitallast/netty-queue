package org.mitallast.queue.common.settings;

import org.mitallast.queue.common.Booleans;
import org.mitallast.queue.common.property.PropertyPlaceholder;
import org.mitallast.queue.common.settings.loader.SettingsLoader;
import org.mitallast.queue.common.settings.loader.SettingsLoaderFactory;
import org.mitallast.queue.common.strings.Strings;
import org.mitallast.queue.common.unit.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.mitallast.queue.common.strings.Strings.toCamelCase;
import static org.mitallast.queue.common.unit.ByteSizeValue.parseBytesSizeValue;
import static org.mitallast.queue.common.unit.SizeValue.parseSizeValue;
import static org.mitallast.queue.common.unit.TimeValue.parseTimeValue;

@SuppressWarnings("unused")
public class ImmutableSettings implements Settings {
    public static final Settings EMPTY = new Builder().build();

    private Map<String, String> settings;
    private transient ClassLoader classLoader;

    ImmutableSettings(Map<String, String> settings, ClassLoader classLoader) {
        this.settings = Collections.unmodifiableMap(settings);
        this.classLoader = classLoader;
    }

    @Override
    public Map<String, String> getAsMap() {
        return this.settings;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> getAsStructuredMap() {
        Map<String, Object> map = new HashMap<>(2);
        for (Map.Entry<String, String> entry : settings.entrySet()) {
            processSetting(map, "", entry.getKey(), entry.getValue());
        }
        map.entrySet().stream().filter(entry -> entry.getValue() instanceof Map).forEach(entry -> {
            Map<String, Object> valMap = (Map<String, Object>) entry.getValue();
            entry.setValue(convertMapsToArrays(valMap));
        });

        return map;
    }

    private void processSetting(Map<String, Object> map, String prefix, String setting, String value) {
        int prefixLength = setting.indexOf('.');
        if (prefixLength == -1) {
            @SuppressWarnings("unchecked") Map<String, Object> innerMap = (Map<String, Object>) map.get(prefix + setting);
            if (innerMap != null) {
                // It supposed to be a value, but we already have a map stored, need to convert this map to "." notation
                for (Map.Entry<String, Object> entry : innerMap.entrySet()) {
                    map.put(prefix + setting + "." + entry.getKey(), entry.getValue());
                }
            }
            map.put(prefix + setting, value);
        } else {
            String key = setting.substring(0, prefixLength);
            String rest = setting.substring(prefixLength + 1);
            Object existingValue = map.get(prefix + key);
            if (existingValue == null) {
                Map<String, Object> newMap = new HashMap<>(2);
                processSetting(newMap, "", rest, value);
                map.put(key, newMap);
            } else {
                if (existingValue instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> innerMap = (Map<String, Object>) existingValue;
                    processSetting(innerMap, "", rest, value);
                    map.put(key, innerMap);
                } else {
                    // It supposed to be a map, but we already have a value stored, which is not a map
                    // fall back to "." notation
                    processSetting(map, prefix + key + ".", rest, value);
                }
            }
        }
    }

    private Object convertMapsToArrays(Map<String, Object> map) {
        if (map.isEmpty()) {
            return map;
        }
        boolean isArray = true;
        int maxIndex = -1;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (isArray) {
                try {
                    int index = Integer.parseInt(entry.getKey());
                    if (index >= 0) {
                        maxIndex = Math.max(maxIndex, index);
                    } else {
                        isArray = false;
                    }
                } catch (NumberFormatException ex) {
                    isArray = false;
                }
            }
            if (entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked") Map<String, Object> valMap = (Map<String, Object>) entry.getValue();
                entry.setValue(convertMapsToArrays(valMap));
            }
        }
        if (isArray && (maxIndex + 1) == map.size()) {
            ArrayList<Object> newValue = new ArrayList<>(maxIndex + 1);
            for (int i = 0; i <= maxIndex; i++) {
                Object obj = map.get(Integer.toString(i));
                if (obj == null) {
                    // Something went wrong. Different format?
                    // Bailout!
                    return map;
                }
                newValue.add(obj);
            }
            return newValue;
        }
        return map;
    }

    @Override
    public Settings getComponentSettings(Class component) {
        if (component.getName().startsWith("org.mitallast.queue")) {
            return getComponentSettings("org.mitallast.queue", component);
        }
        // not starting with org.mitallast.queue, just remove the first package part (probably org/net/com)
        return getComponentSettings(component.getName().substring(0, component.getName().indexOf('.')), component);
    }

    @Override
    public Settings getComponentSettings(String prefix, Class component) {
        String type = component.getName();
        if (!type.startsWith(prefix)) {
            throw new SettingsException("Component [" + type + "] does not start with prefix [" + prefix + "]");
        }
        String settingPrefix = type.substring(prefix.length() + 1); // 1 for the '.'
        settingPrefix = settingPrefix.substring(0, settingPrefix.length() - component.getSimpleName().length()); // remove the simple class name (keep the dot)
        return getByPrefix(settingPrefix);
    }

    @Override
    public Settings getByPrefix(String prefix) {
        Builder builder = new Builder();
        for (Map.Entry<String, String> entry : getAsMap().entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                if (entry.getKey().length() < prefix.length()) {
                    // ignore this one
                    continue;
                }
                builder.put(entry.getKey().substring(prefix.length()), entry.getValue());
            }
        }
        builder.classLoader(classLoader);
        return builder.build();
    }

    @Override
    public String get(String setting) {
        String retVal = settings.get(setting);
        if (retVal != null) {
            return retVal;
        }
        // try camel case version
        return settings.get(toCamelCase(setting));
    }

    @Override
    public String get(String[] settings) {
        for (String setting : settings) {
            String retVal = this.settings.get(setting);
            if (retVal != null) {
                return retVal;
            }
            retVal = this.settings.get(toCamelCase(setting));
            if (retVal != null) {
                return retVal;
            }
        }
        return null;
    }

    @Override
    public String get(String setting, String defaultValue) {
        String retVal = get(setting);
        return retVal == null ? defaultValue : retVal;
    }

    @Override
    public String get(String[] settings, String defaultValue) {
        String retVal = get(settings);
        return retVal == null ? defaultValue : retVal;
    }

    @Override
    public Float getAsFloat(String setting, Float defaultValue) {
        String sValue = get(setting);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Float.parseFloat(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException("Failed to parse float setting [" + setting + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public Float getAsFloat(String[] settings, Float defaultValue) throws SettingsException {
        String sValue = get(settings);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Float.parseFloat(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException("Failed to parse float setting [" + Arrays.toString(settings) + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public Double getAsDouble(String setting, Double defaultValue) {
        String sValue = get(setting);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException("Failed to parse double setting [" + setting + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public Double getAsDouble(String[] settings, Double defaultValue) {
        String sValue = get(settings);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException("Failed to parse double setting [" + Arrays.toString(settings) + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public Integer getAsInt(String setting, Integer defaultValue) {
        String sValue = get(setting);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException("Failed to parse int setting [" + setting + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public Integer getAsInt(String[] settings, Integer defaultValue) {
        String sValue = get(settings);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException("Failed to parse int setting [" + Arrays.toString(settings) + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public Long getAsLong(String setting, Long defaultValue) {
        String sValue = get(setting);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException("Failed to parse long setting [" + setting + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public Long getAsLong(String[] settings, Long defaultValue) {
        String sValue = get(settings);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException("Failed to parse long setting [" + Arrays.toString(settings) + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public Boolean getAsBoolean(String setting, Boolean defaultValue) {
        return Booleans.parseBoolean(get(setting), defaultValue);
    }

    @Override
    public Boolean getAsBoolean(String[] settings, Boolean defaultValue) {
        return Booleans.parseBoolean(get(settings), defaultValue);
    }

    @Override
    public TimeValue getAsTime(String setting, TimeValue defaultValue) {
        return parseTimeValue(get(setting), defaultValue);
    }

    @Override
    public TimeValue getAsTime(String[] settings, TimeValue defaultValue) {
        return parseTimeValue(get(settings), defaultValue);
    }

    @Override
    public ByteSizeValue getAsBytesSize(String setting, ByteSizeValue defaultValue) throws SettingsException {
        return parseBytesSizeValue(get(setting), defaultValue);
    }

    @Override
    public ByteSizeValue getAsBytesSize(String[] settings, ByteSizeValue defaultValue) throws SettingsException {
        return parseBytesSizeValue(get(settings), defaultValue);
    }

    @Override
    public ByteSizeValue getAsMemory(String setting, String defaultValue) throws SettingsException {
        return MemorySizeValue.parseBytesSizeValueOrHeapRatio(get(setting, defaultValue));
    }

    @Override
    public ByteSizeValue getAsMemory(String[] settings, String defaultValue) throws SettingsException {
        return MemorySizeValue.parseBytesSizeValueOrHeapRatio(get(settings, defaultValue));
    }

    @Override
    public SizeValue getAsSize(String setting, SizeValue defaultValue) throws SettingsException {
        return parseSizeValue(get(setting), defaultValue);
    }

    @Override
    public SizeValue getAsSize(String[] settings, SizeValue defaultValue) throws SettingsException {
        return parseSizeValue(get(settings), defaultValue);
    }

    @Override
    public String[] getAsArray(String settingPrefix) throws SettingsException {
        return getAsArray(settingPrefix, Strings.EMPTY_ARRAY, true);
    }

    @Override
    public String[] getAsArray(String settingPrefix, String[] defaultArray) throws SettingsException {
        return getAsArray(settingPrefix, defaultArray, true);
    }

    @Override
    public String[] getAsArray(String settingPrefix, String[] defaultArray, Boolean commaDelimited) throws SettingsException {
        List<String> result = new ArrayList<>();

        if (get(settingPrefix) != null) {
            if (commaDelimited) {
                String[] strings = Strings.splitStringByCommaToArray(get(settingPrefix));
                if (strings.length > 0) {
                    for (String string : strings) {
                        result.add(string.trim());
                    }
                }
            } else {
                result.add(get(settingPrefix).trim());
            }
        }

        int counter = 0;
        while (true) {
            String value = get(settingPrefix + '.' + (counter++));
            if (value == null) {
                break;
            }
            result.add(value.trim());
        }
        if (result.isEmpty()) {
            return defaultArray;
        }
        return result.toArray(new String[result.size()]);
    }

    @Override
    public Map<String, Settings> getGroups(String settingPrefix) throws SettingsException {
        return getGroups(settingPrefix, false);
    }

    @Override
    public Map<String, Settings> getGroups(String settingPrefix, boolean ignoreNonGrouped) throws SettingsException {
        if (!Strings.hasLength(settingPrefix)) {
            throw new IllegalArgumentException("illegal setting prefix " + settingPrefix);
        }
        if (settingPrefix.charAt(settingPrefix.length() - 1) != '.') {
            settingPrefix = settingPrefix + ".";
        }
        // we don't really care that it might happen twice
        Map<String, Map<String, String>> map = new LinkedHashMap<>();
        for (Object o : settings.keySet()) {
            String setting = (String) o;
            if (setting.startsWith(settingPrefix)) {
                String nameValue = setting.substring(settingPrefix.length());
                int dotIndex = nameValue.indexOf('.');
                if (dotIndex == -1) {
                    if (ignoreNonGrouped) {
                        continue;
                    }
                    throw new SettingsException("Failed to get setting group for [" + settingPrefix + "] setting prefix and setting [" + setting + "] because of a missing '.'");
                }
                String name = nameValue.substring(0, dotIndex);
                String value = nameValue.substring(dotIndex + 1);
                Map<String, String> groupSettings = map.get(name);
                if (groupSettings == null) {
                    groupSettings = new LinkedHashMap<>();
                    map.put(name, groupSettings);
                }
                groupSettings.put(value, get(setting));
            }
        }
        Map<String, Settings> retVal = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, String>> entry : map.entrySet()) {
            retVal.put(entry.getKey(), new ImmutableSettings(Collections.unmodifiableMap(entry.getValue()), classLoader));
        }
        return Collections.unmodifiableMap(retVal);
    }

    @Override
    public String toDelimitedString(char delimiter) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : settings.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append(delimiter);
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ImmutableSettings that = (ImmutableSettings) o;

        return !(classLoader != null ? !classLoader.equals(that.classLoader) : that.classLoader != null)
            && !(settings != null ? !settings.equals(that.settings) : that.settings != null);

    }

    @Override
    public int hashCode() {
        int result = settings != null ? settings.hashCode() : 0;
        result = 31 * result + (classLoader != null ? classLoader.hashCode() : 0);
        return result;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder allowing to put different settings and then {@link #build()} an immutable
     * settings implementation. Use {@link ImmutableSettings#builder()} in order to
     * construct it.
     */
    public static class Builder implements Settings.Builder {

        private final Map<String, String> map = new LinkedHashMap<>();
        private ClassLoader classLoader;

        private Builder() {
        }

        public Map<String, String> internalMap() {
            return this.map;
        }

        /**
         * Removes the provided setting from the internal map holding the current list of settings.
         */
        public String remove(String key) {
            return map.remove(key);
        }

        /**
         * Returns a setting value based on the setting key.
         */
        public String get(String key) {
            String retVal = map.get(key);
            if (retVal != null) {
                return retVal;
            }
            // try camel case version
            return map.get(toCamelCase(key));
        }

        /**
         * Puts tuples of key value pairs of settings. Simplified version instead of repeating calling
         * put for each one.
         */
        @SuppressWarnings("unchecked")
        public Builder put(Object... settings) {
            if (settings.length == 1) {
                // support cases where the actual type gets lost down the road...
                Object setting = settings[0];
                if (setting instanceof Map) {
                    //noinspection unchecked
                    return put((Map<String, String>) setting);
                } else if (setting instanceof Settings) {
                    return put((Settings) setting);
                }
            }
            if ((settings.length % 2) != 0) {
                throw new IllegalArgumentException("array settings of key + value order doesn't hold correct number of arguments (" + settings.length + ")");
            }
            for (int i = 0; i < settings.length; i++) {
                put(settings[i++].toString(), settings[i].toString());
            }
            return this;
        }

        /**
         * Sets a setting with the provided setting key and value.
         *
         * @param key   The setting key
         * @param value The setting value
         * @return The builder
         */
        public Builder put(String key, String value) {
            map.put(key, value);
            return this;
        }

        /**
         * Sets a setting with the provided setting key and class as value.
         *
         * @param key   The setting key
         * @param clazz The setting class value
         * @return The builder
         */
        public Builder put(String key, Class clazz) {
            map.put(key, clazz.getName());
            return this;
        }

        /**
         * Sets the setting with the provided setting key and the boolean value.
         *
         * @param setting The setting key
         * @param value   The boolean value
         * @return The builder
         */
        public Builder put(String setting, boolean value) {
            put(setting, String.valueOf(value));
            return this;
        }

        /**
         * Sets the setting with the provided setting key and the int value.
         *
         * @param setting The setting key
         * @param value   The int value
         * @return The builder
         */
        public Builder put(String setting, int value) {
            put(setting, String.valueOf(value));
            return this;
        }

        /**
         * Sets the setting with the provided setting key and the long value.
         *
         * @param setting The setting key
         * @param value   The long value
         * @return The builder
         */
        public Builder put(String setting, long value) {
            put(setting, String.valueOf(value));
            return this;
        }

        /**
         * Sets the setting with the provided setting key and the float value.
         *
         * @param setting The setting key
         * @param value   The float value
         * @return The builder
         */
        public Builder put(String setting, float value) {
            put(setting, String.valueOf(value));
            return this;
        }

        /**
         * Sets the setting with the provided setting key and the double value.
         *
         * @param setting The setting key
         * @param value   The double value
         * @return The builder
         */
        public Builder put(String setting, double value) {
            put(setting, String.valueOf(value));
            return this;
        }

        /**
         * Sets the setting with the provided setting key and the time value.
         *
         * @param setting The setting key
         * @param value   The time value
         * @return The builder
         */
        public Builder put(String setting, long value, TimeUnit timeUnit) {
            put(setting, timeUnit.toMillis(value));
            return this;
        }

        /**
         * Sets the setting with the provided setting key and the size value.
         *
         * @param setting The setting key
         * @param value   The size value
         * @return The builder
         */
        public Builder put(String setting, long value, ByteSizeUnit sizeUnit) {
            put(setting, sizeUnit.toBytes(value));
            return this;
        }

        /**
         * Sets the setting with the provided setting key and an array of values.
         *
         * @param setting The setting key
         * @param values  The values
         * @return The builder
         */
        public Builder putArray(String setting, String... values) {
            remove(setting);
            int counter = 0;
            while (true) {
                String value = map.remove(setting + '.' + (counter++));
                if (value == null) {
                    break;
                }
            }
            for (int i = 0; i < values.length; i++) {
                put(setting + "." + i, values[i]);
            }
            return this;
        }

        /**
         * Sets the setting group.
         */
        public Builder put(String settingPrefix, String groupName, String[] settings, String[] values) throws SettingsException {
            if (settings.length != values.length) {
                throw new SettingsException("The settings length must match the value length");
            }
            for (int i = 0; i < settings.length; i++) {
                if (values[i] == null) {
                    continue;
                }
                put(settingPrefix + "." + groupName + "." + settings[i], values[i]);
            }
            return this;
        }

        /**
         * Sets all the provided settings.
         */
        public Builder put(Settings settings) {
            map.putAll(settings.getAsMap());
            return this;
        }

        /**
         * Sets all the provided settings.
         */
        public Builder put(Map<String, String> settings) {
            map.putAll(settings);
            return this;
        }

        /**
         * Sets all the provided settings.
         */
        public Builder put(Properties properties) {
            for (Map.Entry entry : properties.entrySet()) {
                map.put((String) entry.getKey(), (String) entry.getValue());
            }
            return this;
        }

        public Builder loadFromDelimitedString(String value, char delimiter) {
            String[] values = Strings.splitStringToArray(value, delimiter);
            for (String s : values) {
                int index = s.indexOf('=');
                if (index == -1) {
                    throw new IllegalArgumentException("value [" + s + "] for settings loaded with delimiter [" + delimiter + "] is malformed, missing =");
                }
                map.put(s.substring(0, index), s.substring(index + 1));
            }
            return this;
        }

        public Builder loadFromUrl(URL url) throws SettingsException {
            try {
                return loadFromStream(url.toExternalForm(), url.openStream());
            } catch (IOException e) {
                throw new SettingsException("Failed to open stream for url [" + url.toExternalForm() + "]", e);
            }
        }

        public Builder loadFromStream(String resourceName, InputStream inputStream) throws SettingsException {
            SettingsLoader settingsLoader = SettingsLoaderFactory.loaderFromResource(resourceName);
            try {
                Map<String, String> loadedSettings = settingsLoader.load(inputStream);
                put(loadedSettings);
            } catch (Exception e) {
                throw new SettingsException("Failed to load settings from [" + resourceName + "]", e);
            }
            return this;
        }

        public Builder loadFromClasspath(String resourceName) throws SettingsException {
            InputStream is = getClass().getResourceAsStream(resourceName);
            if (is == null) {
                return this;
            }

            return loadFromStream(resourceName, is);
        }

        /**
         * Sets the class loader associated with the settings built.
         */
        public Builder classLoader(ClassLoader classLoader) {
            this.classLoader = classLoader;
            return this;
        }

        /**
         * Puts all the properties with keys starting with the provided <tt>prefix</tt>.
         *
         * @param prefix     The prefix to filter property key by
         * @param properties The properties to put
         * @return The builder
         */
        public Builder putProperties(String prefix, Properties properties) {
            for (Object key1 : properties.keySet()) {
                String key = (String) key1;
                String value = properties.getProperty(key);
                if (key.startsWith(prefix)) {
                    map.put(key.substring(prefix.length()), value);
                }
            }
            return this;
        }

        /**
         * Puts all the properties with keys starting with the provided <tt>prefix</tt>.
         *
         * @param prefix     The prefix to filter property key by
         * @param properties The properties to put
         * @return The builder
         */
        public Builder putProperties(String prefix, Properties properties, String[] ignorePrefixes) {
            for (Object key1 : properties.keySet()) {
                String key = (String) key1;
                String value = properties.getProperty(key);
                if (key.startsWith(prefix)) {
                    boolean ignore = false;
                    for (String ignorePrefix : ignorePrefixes) {
                        if (key.startsWith(ignorePrefix)) {
                            ignore = true;
                            break;
                        }
                    }
                    if (!ignore) {
                        map.put(key.substring(prefix.length()), value);
                    }
                }
            }
            return this;
        }

        /**
         * Runs across all the settings set on this builder and replaces <tt>${...}</tt> elements in the
         * each setting value according to the following logic:
         * <p/>
         * <p>First, tries to resolve it against a System property ({@link System#getProperty(String)}), next,
         * tries and resolve it against an environment variable ({@link System#getenv(String)}), and last, tries
         * and replace it with another setting already set on this builder.
         */
        public Builder replacePropertyPlaceholders() {
            PropertyPlaceholder propertyPlaceholder = new PropertyPlaceholder("${", "}", false);
            PropertyPlaceholder.PlaceholderResolver placeholderResolver = new PropertyPlaceholder.PlaceholderResolver() {
                @Override
                public String resolvePlaceholder(String placeholderName) {
                    if (placeholderName.startsWith("env.")) {
                        // explicit env var prefix
                        return System.getenv(placeholderName.substring("env.".length()));
                    }
                    String value = System.getProperty(placeholderName);
                    if (value != null) {
                        return value;
                    }
                    value = System.getenv(placeholderName);
                    if (value != null) {
                        return value;
                    }
                    return map.get(placeholderName);
                }

                @Override
                public boolean shouldIgnoreMissing(String placeholderName) {
                    // if its an explicit env var, we are ok with not having a value for it and treat it as optional
                    return placeholderName.startsWith("env.");
                }
            };
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String value = propertyPlaceholder.replacePlaceholders(entry.getValue(), placeholderResolver);
                // if the values exists and has length, we should maintain it  in the map
                // otherwise, the replace process resolved into removing it
                if (Strings.hasLength(value)) {
                    map.put(entry.getKey(), value);
                } else {
                    map.remove(entry.getKey());
                }
            }
            return this;
        }

        /**
         * Builds a {@link Settings} (underlying uses {@link ImmutableSettings}) based on everything
         * set on this builder.
         */
        public Settings build() {
            return new ImmutableSettings(Collections.unmodifiableMap(map), classLoader);
        }
    }
}
