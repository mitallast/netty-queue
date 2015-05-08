package org.mitallast.queue.common.logging;

import org.mitallast.queue.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

public class Loggers {

    public static final String SPACE = " ";

    public static Logger getLogger(Class className, Settings settings) {
        return getLogger(className, settings, null);
    }

    public static Logger getLogger(Class className, Settings settings, String... prefixes) {
        String prefix = "";
        StringBuilder sb = null;
        String name = settings.get("node.name");
        if (name != null) {
            sb = new StringBuilder();
            sb.append('[').append(name).append(']');
        }

        if (prefixes != null && prefixes.length > 0) {
            if (sb == null) {
                sb = new StringBuilder();
            }
            for (String prefixX : prefixes) {
                if (prefixX != null) {
                    sb.append('[').append(prefixX).append(']');
                }
            }
        }
        if (sb != null && sb.length() > 0) {
            sb.append(" ");
            prefix = sb.toString();
        }
        return new SLF4JLogger(LoggerFactory.getLogger(className), prefix);
    }

    private static class SLF4JLogger implements Logger {

        private final Logger logger;
        private final String prefix;

        private SLF4JLogger(Logger logger, String prefix) {
            this.logger = logger;
            this.prefix = prefix;
        }

        @Override
        public String getName() {
            return logger.getName();
        }

        @Override
        public boolean isTraceEnabled() {
            return logger.isTraceEnabled();
        }

        @Override
        public void trace(String s) {
            logger.trace(prefix + s);
        }

        @Override
        public void trace(String s, Object o) {
            logger.trace(prefix + s, o);
        }

        @Override
        public void trace(String s, Object o, Object o1) {
            logger.trace(prefix + s, o, o1);
        }

        @Override
        public void trace(String s, Object... objects) {
            logger.trace(prefix + s, objects);
        }

        @Override
        public void trace(String s, Throwable throwable) {
            logger.trace(prefix + s, throwable);
        }

        @Override
        public boolean isTraceEnabled(Marker marker) {
            return logger.isTraceEnabled(marker);
        }

        @Override
        public void trace(Marker marker, String s) {
            logger.trace(marker, prefix + s);
        }

        @Override
        public void trace(Marker marker, String s, Object o) {
            logger.trace(marker, prefix + s, o);
        }

        @Override
        public void trace(Marker marker, String s, Object o, Object o1) {
            logger.trace(marker, prefix + s, o, o1);
        }

        @Override
        public void trace(Marker marker, String s, Object... objects) {
            logger.trace(marker, prefix + s, objects);
        }

        @Override
        public void trace(Marker marker, String s, Throwable throwable) {
            logger.trace(marker, prefix + s, throwable);
        }

        @Override
        public boolean isDebugEnabled() {
            return logger.isDebugEnabled();
        }

        @Override
        public void debug(String s) {
            logger.debug(prefix + s);
        }

        @Override
        public void debug(String s, Object o) {
            logger.debug(prefix + s, o);
        }

        @Override
        public void debug(String s, Object o, Object o1) {
            logger.debug(prefix + s, o, o1);
        }

        @Override
        public void debug(String s, Object... objects) {
            logger.debug(prefix + s, objects);
        }

        @Override
        public void debug(String s, Throwable throwable) {
            logger.debug(prefix + s, throwable);
        }

        @Override
        public boolean isDebugEnabled(Marker marker) {
            return logger.isDebugEnabled(marker);
        }

        @Override
        public void debug(Marker marker, String s) {
            logger.debug(marker, prefix + s);
        }

        @Override
        public void debug(Marker marker, String s, Object o) {
            logger.debug(marker, prefix + s, o);
        }

        @Override
        public void debug(Marker marker, String s, Object o, Object o1) {
            logger.debug(marker, prefix + s, o, o1);
        }

        @Override
        public void debug(Marker marker, String s, Object... objects) {
            logger.debug(marker, prefix + s, objects);
        }

        @Override
        public void debug(Marker marker, String s, Throwable throwable) {
            logger.debug(marker, prefix + s, throwable);
        }

        @Override
        public boolean isInfoEnabled() {
            return logger.isInfoEnabled();
        }

        @Override
        public void info(String s) {
            logger.info(prefix + s);
        }

        @Override
        public void info(String s, Object o) {
            logger.info(prefix + s, o);
        }

        @Override
        public void info(String s, Object o, Object o1) {
            logger.info(prefix + s, o, o1);
        }

        @Override
        public void info(String s, Object... objects) {
            logger.info(prefix + s, objects);
        }

        @Override
        public void info(String s, Throwable throwable) {
            logger.info(prefix + s, throwable);
        }

        @Override
        public boolean isInfoEnabled(Marker marker) {
            return logger.isInfoEnabled(marker);
        }

        @Override
        public void info(Marker marker, String s) {
            logger.info(marker, prefix + s);
        }

        @Override
        public void info(Marker marker, String s, Object o) {
            logger.info(marker, prefix + s, o);
        }

        @Override
        public void info(Marker marker, String s, Object o, Object o1) {
            logger.info(marker, prefix + s, o, o1);
        }

        @Override
        public void info(Marker marker, String s, Object... objects) {
            logger.info(marker, prefix + s, objects);
        }

        @Override
        public void info(Marker marker, String s, Throwable throwable) {
            logger.info(marker, prefix + s, throwable);
        }

        @Override
        public boolean isWarnEnabled() {
            return logger.isWarnEnabled();
        }

        @Override
        public void warn(String s) {
            logger.warn(prefix + s);
        }

        @Override
        public void warn(String s, Object o) {
            logger.warn(prefix + s, o);
        }

        @Override
        public void warn(String s, Object... objects) {
            logger.warn(prefix + s, objects);
        }

        @Override
        public void warn(String s, Object o, Object o1) {
            logger.warn(prefix + s, o, o1);
        }

        @Override
        public void warn(String s, Throwable throwable) {
            logger.warn(prefix + s, throwable);
        }

        @Override
        public boolean isWarnEnabled(Marker marker) {
            return logger.isWarnEnabled(marker);
        }

        @Override
        public void warn(Marker marker, String s) {
            logger.warn(marker, prefix + s);
        }

        @Override
        public void warn(Marker marker, String s, Object o) {
            logger.warn(marker, prefix + s, o);
        }

        @Override
        public void warn(Marker marker, String s, Object o, Object o1) {
            logger.warn(marker, prefix + s, o, o1);
        }

        @Override
        public void warn(Marker marker, String s, Object... objects) {
            logger.warn(marker, prefix + s, objects);
        }

        @Override
        public void warn(Marker marker, String s, Throwable throwable) {
            logger.warn(marker, prefix + s, throwable);
        }

        @Override
        public boolean isErrorEnabled() {
            return logger.isErrorEnabled();
        }

        @Override
        public void error(String s) {
            logger.error(prefix + s);
        }

        @Override
        public void error(String s, Object o) {
            logger.error(prefix + s, o);
        }

        @Override
        public void error(String s, Object o, Object o1) {
            logger.error(prefix + s, o, o1);
        }

        @Override
        public void error(String s, Object... objects) {
            logger.error(prefix + s, objects);
        }

        @Override
        public void error(String s, Throwable throwable) {
            logger.error(prefix + s, throwable);
        }

        @Override
        public boolean isErrorEnabled(Marker marker) {
            return logger.isErrorEnabled(marker);
        }

        @Override
        public void error(Marker marker, String s) {
            logger.error(marker, prefix + s);
        }

        @Override
        public void error(Marker marker, String s, Object o) {
            logger.error(marker, prefix + s, o);
        }

        @Override
        public void error(Marker marker, String s, Object o, Object o1) {
            logger.error(marker, prefix + s, o, o1);
        }

        @Override
        public void error(Marker marker, String s, Object... objects) {
            logger.error(marker, prefix + s, objects);
        }

        @Override
        public void error(Marker marker, String s, Throwable throwable) {
            logger.error(marker, prefix + s, throwable);
        }
    }
}
