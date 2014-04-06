package org.mitallast.queue.common.settings;

import org.mitallast.queue.common.unit.ByteSizeValue;
import org.mitallast.queue.common.unit.SizeValue;
import org.mitallast.queue.common.unit.TimeValue;

import java.util.Map;

public interface Settings {
    /**
     * Component settings for a specific component. Returns all the settings for the given class, where the
     * FQN of the class is used, without the <tt>org.mitallast<tt> prefix. If there is no <tt>org.mitallast</tt>
     * prefix, then the prefix used is the first part of the package name (<tt>org</tt> / <tt>com</tt> / ...)
     */
    Settings getComponentSettings(Class component);

    /**
     * Component settings for a specific component. Returns all the settings for the given class, where the
     * FQN of the class is used, without provided prefix.
     */
    Settings getComponentSettings(String prefix, Class component);

    /**
     * A settings that are filtered (and key is removed) with the specified prefix.
     */
    Settings getByPrefix(String prefix);

    /**
     * The settings as a flat {@link java.util.Map}.
     */
    Map<String, String> getAsMap();

    /**
     * The settings as a structured {@link java.util.Map}.
     */
    Map<String, Object> getAsStructuredMap();

    /**
     * Returns the setting value associated with the setting key.
     *
     * @param setting The setting key
     * @return The setting value, <tt>null</tt> if it does not exists.
     */
    String get(String setting);

    /**
     * Returns the setting value associated with the first setting key.
     */
    String get(String[] settings);

    /**
     * Returns the setting value associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    String get(String setting, String defaultValue);

    /**
     * Returns the setting value associated with the first setting key, if none exists,
     * returns the default value provided.
     */
    String get(String[] settings, String defaultValue);

    /**
     * Returns group settings for the given setting prefix.
     */
    Map<String, Settings> getGroups(String settingPrefix) throws SettingsException;

    /**
     * Returns group settings for the given setting prefix.
     */
    Map<String, Settings> getGroups(String settingPrefix, boolean ignoreNonGrouped) throws SettingsException;

    /**
     * Returns the setting value (as float) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    Float getAsFloat(String setting, Float defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as float) associated with teh first setting key, if none
     * exists, returns the default value provided.
     */
    Float getAsFloat(String[] settings, Float defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as double) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    Double getAsDouble(String setting, Double defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as double) associated with teh first setting key, if none
     * exists, returns the default value provided.
     */
    Double getAsDouble(String[] settings, Double defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as int) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    Integer getAsInt(String setting, Integer defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as int) associated with the first setting key. If it does not exists,
     * returns the default value provided.
     */
    Integer getAsInt(String[] settings, Integer defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as long) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    Long getAsLong(String setting, Long defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as long) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    Long getAsLong(String[] settings, Long defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as boolean) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    Boolean getAsBoolean(String setting, Boolean defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as boolean) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    Boolean getAsBoolean(String[] settings, Boolean defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as time) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    TimeValue getAsTime(String setting, TimeValue defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as time) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    TimeValue getAsTime(String[] settings, TimeValue defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as size) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    ByteSizeValue getAsBytesSize(String setting, ByteSizeValue defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as size) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    ByteSizeValue getAsBytesSize(String[] settings, ByteSizeValue defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as size) associated with the setting key. Provided values can either be
     * absolute values (intepreted as a number of bytes), byte sizes (eg. 1mb) or percentage of the heap size
     * (eg. 12%). If it does not exists, parses the default value provided.
     */
    ByteSizeValue getAsMemory(String setting, String defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as size) associated with the setting key. Provided values can either be
     * absolute values (intepreted as a number of bytes), byte sizes (eg. 1mb) or percentage of the heap size
     * (eg. 12%). If it does not exists, parses the default value provided.
     */
    ByteSizeValue getAsMemory(String[] setting, String defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as size) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    SizeValue getAsSize(String setting, SizeValue defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as size) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    SizeValue getAsSize(String[] settings, SizeValue defaultValue) throws SettingsException;

    /**
     * The values associated with a setting prefix as an array. The settings array is in the format of:
     * <tt>settingPrefix.[index]</tt>.
     * <p/>
     * <p>It will also automatically load a comma separated list under the settingPrefix and merge with
     * the numbered format.
     *
     * @param settingPrefix  The setting prefix to load the array by
     * @param defaultArray   The default array to use if no value is specified
     * @param commaDelimited Whether to try to parse a string as a comma-delimited value
     * @return The setting array values
     * @throws SettingsException
     */
    String[] getAsArray(String settingPrefix, String[] defaultArray, Boolean commaDelimited) throws SettingsException;

    /**
     * The values associated with a setting prefix as an array. The settings array is in the format of:
     * <tt>settingPrefix.[index]</tt>.
     * <p/>
     * <p>If commaDelimited is true, it will automatically load a comma separated list under the settingPrefix and merge with
     * the numbered format.
     *
     * @param settingPrefix The setting prefix to load the array by
     * @return The setting array values
     * @throws SettingsException
     */
    String[] getAsArray(String settingPrefix, String[] defaultArray) throws SettingsException;

    /**
     * The values associated with a setting prefix as an array. The settings array is in the format of:
     * <tt>settingPrefix.[index]</tt>.
     * <p/>
     * <p>It will also automatically load a comma separated list under the settingPrefix and merge with
     * the numbered format.
     *
     * @param settingPrefix The setting prefix to load the array by
     * @return The setting array values
     * @throws SettingsException
     */
    String[] getAsArray(String settingPrefix) throws SettingsException;

    /**
     * Returns the settings as delimited string.
     */
    String toDelimitedString(char delimiter);

    interface Builder {

        /**
         * Builds the settings.
         */
        Settings build();
    }
}
