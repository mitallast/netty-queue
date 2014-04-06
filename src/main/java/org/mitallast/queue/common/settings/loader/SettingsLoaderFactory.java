package org.mitallast.queue.common.settings.loader;

public final class SettingsLoaderFactory {

    private SettingsLoaderFactory() {

    }

    /**
     * Returns a {@link SettingsLoader} based on the resource name.
     */
    public static SettingsLoader loaderFromResource(String resourceName) {
        if (resourceName.endsWith(".json")) {
            return new JsonSettingsLoader();
        } else if (resourceName.endsWith(".properties") || resourceName.endsWith(".ini")) {
            return new PropertiesSettingsLoader();
        } else {
            return new JsonSettingsLoader();
        }
    }

    /**
     * Returns a {@link SettingsLoader} based on the actual settings source.
     */
    public static SettingsLoader loaderFromSource(String source) {
        if (source.indexOf('{') != -1 && source.indexOf('}') != -1) {
            return new JsonSettingsLoader();
        }
        return new PropertiesSettingsLoader();
    }
}
