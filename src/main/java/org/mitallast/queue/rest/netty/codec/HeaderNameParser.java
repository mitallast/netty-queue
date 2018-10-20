package org.mitallast.queue.rest.netty.codec;

import io.netty.handler.codec.http.HttpConstants;
import io.netty.util.ByteProcessor;

class HeaderNameParser implements ByteProcessor {
    private final byte[] buffer = new byte[255];
    private int pos = 0;
    private boolean endOfHeaders = false;

    public void reset() {
        pos = 0;
        endOfHeaders = false;
    }

    public boolean endOfHeaders() {
        return endOfHeaders;
    }

    @Override
    public boolean process(byte value) throws Exception {
        if (value == HttpConstants.COLON) {
            return false;
        }
        if (value == HttpConstants.SP) {
            return true;
        }

        if (value == HttpConstants.CR) {
            return true;
        }
        if (value == HttpConstants.LF) {
            endOfHeaders = true;
            return false;
        }

        buffer[pos] = AsciiString.toLowerCase(value);
        pos++;
        return true;
    }

    public HttpHeaderName find() {
        switch (buffer[0]) {
            case 'a':
                return parseA();
            case 'c':
                return parseC();
            case 'd':
                if (buffer[1] == 'a'
                    && buffer[2] == 't'
                    && buffer[3] == 'e'
                ) return HttpHeaderName.DATE;
                break;
            case 'e':
                return parseE();
            case 'f':
                if (buffer[1] == 'r'
                    && buffer[2] == 'o'
                    && buffer[3] == 'm'
                ) return HttpHeaderName.FROM;
                break;
            case 'h':
                if (buffer[1] == 'o'
                    && buffer[2] == 's'
                    && buffer[3] == 't'
                ) return HttpHeaderName.HOST;
                break;
            case 'i':
                return parseI();
            case 'k':
                if (buffer[1] == 'e' &&
                    buffer[2] == 'e' &&
                    buffer[3] == 'p' &&
                    buffer[4] == '-' &&
                    buffer[5] == 'a' &&
                    buffer[6] == 'l' &&
                    buffer[7] == 'i' &&
                    buffer[8] == 'v' &&
                    buffer[9] == 'e'
                ) return HttpHeaderName.KEEP_ALIVE;
                break;
            case 'l':
                switch (buffer[1]) {
                    case 'a':
                        if (buffer[2] == 's' &&
                            buffer[3] == 't' &&
                            buffer[4] == '-' &&
                            buffer[5] == 'm' &&
                            buffer[6] == 'o' &&
                            buffer[7] == 'd' &&
                            buffer[8] == 'i' &&
                            buffer[9] == 'f' &&
                            buffer[10] == 'i' &&
                            buffer[11] == 'e' &&
                            buffer[12] == 'd'
                        ) return HttpHeaderName.LAST_MODIFIED;
                        break;
                    case 'o':
                        if (buffer[2] == 'c' &&
                            buffer[3] == 'a' &&
                            buffer[4] == 't' &&
                            buffer[5] == 'i' &&
                            buffer[6] == 'o' &&
                            buffer[7] == 'n'
                        ) return HttpHeaderName.LOCATION;
                        break;
                }
                break;
            case 'm':
                if (buffer[1] == 'a' &&
                    buffer[2] == 'x' &&
                    buffer[3] == '-' &&
                    buffer[4] == 'f' &&
                    buffer[5] == 'o' &&
                    buffer[6] == 'r' &&
                    buffer[7] == 'w' &&
                    buffer[8] == 'a' &&
                    buffer[9] == 'r' &&
                    buffer[10] == 'd' &&
                    buffer[11] == 's'
                ) return HttpHeaderName.MAX_FORWARDS;
                break;
            case 'o':
                if (buffer[1] == 'r' &&
                    buffer[2] == 'i' &&
                    buffer[3] == 'g' &&
                    buffer[4] == 'i' &&
                    buffer[5] == 'n'
                ) return HttpHeaderName.ORIGIN;
                break;
            case 'p':
                return parseP();
            case 'r':
                return parseR();
            case 's':
                return parseS();
            case 't':
                return parseT();
            case 'u':
                return parseU();
            case 'v':
                return parseV();
            case 'w':
                return parseW();
            case 'x':
                return parseX();
        }
        return null;
    }

    private HttpHeaderName parseA() {
        switch (buffer[1]) {
            case 'c':
                return parseAC();
            case 'g':
                if (buffer[2] == 'e') return HttpHeaderName.AGE;
                break;
            case 'l':
                if (buffer[2] == 'l'
                    && buffer[3] == 'o'
                    && buffer[4] == 'w'
                ) return HttpHeaderName.ALLOW;
                break;
            case 'u':
                if (buffer[2] == 't'
                    && buffer[3] == 'h'
                    && buffer[4] == 'o'
                    && buffer[5] == 'r'
                    && buffer[6] == 'i'
                    && buffer[7] == 'z'
                    && buffer[8] == 'a'
                    && buffer[9] == 't'
                    && buffer[10] == 'i'
                    && buffer[11] == 'o'
                    && buffer[12] == 'n'
                ) return HttpHeaderName.AUTHORIZATION;
                break;
        }
        return null;
    }

    private HttpHeaderName parseAC() {
        if (buffer[2] == 'c' && buffer[3] == 'e') {
            switch (buffer[4]) {
                case 'p':
                    if (buffer[5] == 't' && pos == 6) {
                        return HttpHeaderName.ACCEPT;
                    }
                    if (buffer[5] == 't' && buffer[6] == '-') {// accept_
                        switch (buffer[7]) {
                            case 'c':
                                if (buffer[8] == 'h'
                                    && buffer[9] == 'a'
                                    && buffer[10] == 'r'
                                    && buffer[11] == 's'
                                    && buffer[12] == 'e'
                                    && buffer[13] == 't'
                                ) return HttpHeaderName.ACCEPT_CHARSET;
                                break;
                            case 'e':
                                if (buffer[8] == 'n'
                                    && buffer[9] == 'c'
                                    && buffer[10] == 'o'
                                    && buffer[11] == 'd'
                                    && buffer[12] == 'i'
                                    && buffer[13] == 'n'
                                    && buffer[14] == 'g'
                                ) return HttpHeaderName.ACCEPT_ENCODING;
                                break;
                            case 'l':
                                if (buffer[8] == 'a'
                                    && buffer[9] == 'n'
                                    && buffer[10] == 'g'
                                    && buffer[11] == 'u'
                                    && buffer[12] == 'a'
                                    && buffer[13] == 'g'
                                    && buffer[14] == 'e'
                                ) return HttpHeaderName.ACCEPT_LANGUAGE;
                                break;
                            case 'r':
                                if (buffer[8] == 'a'
                                    && buffer[9] == 'n'
                                    && buffer[10] == 'g'
                                    && buffer[11] == 'e'
                                    && buffer[12] == 's'
                                ) return HttpHeaderName.ACCEPT_RANGES;
                                break;
                            case 'p':
                                if (buffer[8] == 'a'
                                    && buffer[9] == 't'
                                    && buffer[10] == 'c'
                                    && buffer[11] == 'h'
                                ) return HttpHeaderName.ACCEPT_PATCH;
                                break;
                        }
                    }
                    break;
                case 's':
                    // access_control_
                    if (buffer[5] == 's'
                        && buffer[6] == '-'
                        && buffer[7] == 'c'
                        && buffer[8] == 'o'
                        && buffer[9] == 'n'
                        && buffer[10] == 't'
                        && buffer[11] == 'r'
                        && buffer[12] == 'o'
                        && buffer[13] == 'l'
                        && buffer[14] == '-'
                    ) {
                        switch (buffer[15]) {
                            case 'a':
                                // access_control_allow_
                                if (buffer[16] == 'l'
                                    && buffer[17] == 'l'
                                    && buffer[18] == 'o'
                                    && buffer[19] == 'w'
                                    && buffer[20] == '-'
                                ) {
                                    switch (buffer[21]) {
                                        case 'c':
                                            if (buffer[22] == 'r'
                                                && buffer[23] == 'e'
                                                && buffer[24] == 'd'
                                                && buffer[25] == 'e'
                                                && buffer[26] == 'n'
                                                && buffer[27] == 't'
                                                && buffer[28] == 'i'
                                                && buffer[29] == 'a'
                                                && buffer[30] == 'l'
                                                && buffer[31] == 's'
                                            )
                                                return HttpHeaderName.ACCESS_CONTROL_ALLOW_CREDENTIALS;
                                            break;
                                        case 'h':
                                            if (buffer[22] == 'e'
                                                && buffer[23] == 'a'
                                                && buffer[24] == 'd'
                                                && buffer[25] == 'e'
                                                && buffer[26] == 'r'
                                                && buffer[27] == 's'
                                            ) return HttpHeaderName.ACCESS_CONTROL_ALLOW_HEADERS;
                                            break;
                                        case 'm':
                                            if (buffer[22] == 'e'
                                                && buffer[23] == 't'
                                                && buffer[24] == 'h'
                                                && buffer[25] == 'o'
                                                && buffer[26] == 'd'
                                                && buffer[27] == 's'
                                            ) return HttpHeaderName.ACCESS_CONTROL_ALLOW_METHODS;
                                            break;
                                        case 'o':
                                            if (buffer[22] == 'r'
                                                && buffer[23] == 'i'
                                                && buffer[24] == 'g'
                                                && buffer[25] == 'i'
                                                && buffer[26] == 'n'
                                            ) return HttpHeaderName.ACCESS_CONTROL_ALLOW_ORIGIN;
                                            break;
                                    }
                                }
                                break;
                            case 'e':
                                if (buffer[16] == 'x'
                                    && buffer[17] == 'p'
                                    && buffer[18] == 'o'
                                    && buffer[19] == 's'
                                    && buffer[20] == 'e'
                                    && buffer[21] == '-'
                                    && buffer[22] == 'h'
                                    && buffer[23] == 'e'
                                    && buffer[24] == 'a'
                                    && buffer[25] == 'd'
                                    && buffer[26] == 'e'
                                    && buffer[27] == 'r'
                                    && buffer[28] == 's'
                                ) return HttpHeaderName.ACCESS_CONTROL_EXPOSE_HEADERS;
                                break;
                            case 'm':
                                if (buffer[16] == 'a'
                                    && buffer[17] == 'x'
                                    && buffer[18] == '-'
                                    && buffer[19] == 'a'
                                    && buffer[20] == 'g'
                                    && buffer[21] == 'e'
                                ) return HttpHeaderName.ACCESS_CONTROL_MAX_AGE;
                                break;
                            case 'r':
                                if (buffer[16] == 'e'
                                    && buffer[17] == 'q'
                                    && buffer[18] == 'u'
                                    && buffer[19] == 'e'
                                    && buffer[20] == 's'
                                    && buffer[21] == 't'
                                    && buffer[22] == '-'
                                ) {
                                    if (buffer[23] == 'h'
                                        && buffer[24] == 'e'
                                        && buffer[25] == 'a'
                                        && buffer[26] == 'd'
                                        && buffer[27] == 'e'
                                        && buffer[28] == 'r'
                                        && buffer[29] == 's'
                                    ) return HttpHeaderName.ACCESS_CONTROL_REQUEST_HEADERS;
                                    if (buffer[23] == 'm'
                                        && buffer[24] == 'e'
                                        && buffer[25] == 't'
                                        && buffer[26] == 'h'
                                        && buffer[27] == 'o'
                                        && buffer[28] == 'd'
                                    ) return HttpHeaderName.ACCESS_CONTROL_REQUEST_METHOD;

                                }
                                break;
                        }
                    }
                    break;
            }
        }
        return null;
    }

    private HttpHeaderName parseC() {
        if (buffer[1] == 'a' &&
            buffer[2] == 'c' &&
            buffer[3] == 'h' &&
            buffer[4] == 'e' &&
            buffer[5] == '-' &&
            buffer[6] == 'c' &&
            buffer[7] == 'o' &&
            buffer[8] == 'n' &&
            buffer[9] == 't' &&
            buffer[10] == 'r' &&
            buffer[11] == 'o' &&
            buffer[12] == 'l'
        ) return HttpHeaderName.CACHE_CONTROL;
        if (buffer[1] == 'o') {
            if (buffer[2] == 'n')
                return parseCON();
            if (buffer[2] == 'o' &&
                buffer[3] == 'k' &&
                buffer[4] == 'i' &&
                buffer[5] == 'e'
            ) return HttpHeaderName.COOKIE;
            return null;
        }
        return null;
    }

    private HttpHeaderName parseCON() {
        if (buffer[3] == 'n' &&
            buffer[4] == 'e' &&
            buffer[5] == 'c' &&
            buffer[6] == 't' &&
            buffer[7] == 'i' &&
            buffer[8] == 'o' &&
            buffer[9] == 'n'
        ) return HttpHeaderName.CONNECTION;

        if (buffer[3] == 't' &&
            buffer[4] == 'e' &&
            buffer[5] == 'n' &&
            buffer[6] == 't' &&
            buffer[7] == '-'
        ) { // content-
            if (buffer[8] == 'b' &&
                buffer[9] == 'a' &&
                buffer[10] == 's' &&
                buffer[11] == 'e'
            ) return HttpHeaderName.CONTENT_BASE;

            if (buffer[8] == 'e' &&
                buffer[9] == 'n' &&
                buffer[10] == 'c' &&
                buffer[11] == 'o' &&
                buffer[12] == 'd' &&
                buffer[13] == 'i' &&
                buffer[14] == 'n' &&
                buffer[15] == 'g'
            ) return HttpHeaderName.CONTENT_ENCODING;

            if (buffer[8] == 'l') {
                if (buffer[9] == 'a' &&
                    buffer[10] == 'n' &&
                    buffer[11] == 'g' &&
                    buffer[12] == 'u' &&
                    buffer[13] == 'a' &&
                    buffer[14] == 'g' &&
                    buffer[15] == 'e'
                ) return HttpHeaderName.CONTENT_LANGUAGE;
                if (buffer[9] == 'e' &&
                    buffer[10] == 'n' &&
                    buffer[11] == 'g' &&
                    buffer[12] == 't' &&
                    buffer[13] == 'h'
                ) return HttpHeaderName.CONTENT_LENGTH;

                if (buffer[9] == 'o' &&
                    buffer[10] == 'c' &&
                    buffer[11] == 'a' &&
                    buffer[12] == 't' &&
                    buffer[13] == 'i' &&
                    buffer[14] == 'o' &&
                    buffer[15] == 'n'
                ) return HttpHeaderName.CONTENT_LOCATION;

                return null;
            }

            if (buffer[8] == 't' &&
                buffer[9] == 'r' &&
                buffer[10] == 'a' &&
                buffer[11] == 'n' &&
                buffer[12] == 's' &&
                buffer[13] == 'f' &&
                buffer[14] == 'e' &&
                buffer[15] == 'r' &&
                buffer[16] == '-' &&
                buffer[17] == 'e' &&
                buffer[18] == 'n' &&
                buffer[19] == 'c' &&
                buffer[20] == 'o' &&
                buffer[21] == 'd' &&
                buffer[22] == 'i' &&
                buffer[23] == 'n' &&
                buffer[24] == 'g'
            ) return HttpHeaderName.CONTENT_TRANSFER_ENCODING;

            if (buffer[8] == 'd' &&
                buffer[9] == 'i' &&
                buffer[10] == 's' &&
                buffer[11] == 'p' &&
                buffer[12] == 'o' &&
                buffer[13] == 's' &&
                buffer[14] == 'i' &&
                buffer[15] == 't' &&
                buffer[16] == 'i' &&
                buffer[17] == 'o' &&
                buffer[18] == 'n'
            ) return HttpHeaderName.CONTENT_DISPOSITION;

            if (buffer[8] == 'm' &&
                buffer[9] == 'd' &&
                buffer[10] == '5'
            ) return HttpHeaderName.CONTENT_MD5;


            if (buffer[8] == 'r' &&
                buffer[9] == 'a' &&
                buffer[10] == 'n' &&
                buffer[11] == 'g' &&
                buffer[12] == 'e'
            ) return HttpHeaderName.CONTENT_RANGE;

            if (buffer[8] == 's' &&
                buffer[9] == 'e' &&
                buffer[10] == 'c' &&
                buffer[11] == 'u' &&
                buffer[12] == 'r' &&
                buffer[13] == 'i' &&
                buffer[14] == 't' &&
                buffer[15] == 'y' &&
                buffer[16] == '-' &&
                buffer[17] == 'p' &&
                buffer[18] == 'o' &&
                buffer[19] == 'l' &&
                buffer[20] == 'i' &&
                buffer[21] == 'c' &&
                buffer[22] == 'y'
            ) return HttpHeaderName.CONTENT_SECURITY_POLICY;

            if (buffer[8] == 't' &&
                buffer[9] == 'y' &&
                buffer[10] == 'p' &&
                buffer[11] == 'e'
            ) return HttpHeaderName.CONTENT_TYPE;

            return null;
        }
        return null;
    }

    private HttpHeaderName parseE() {
        switch (buffer[1]) {
            case 't':
                if (buffer[2] == 'a' && buffer[3] == 'g') {
                    return HttpHeaderName.ETAG;
                }
                break;
            case 'x':
                if (buffer[2] == 'p'
                    && buffer[3] == 'e'
                    && buffer[4] == 'c'
                    && buffer[5] == 't'
                ) {
                    return HttpHeaderName.EXPECT;
                }
                if (buffer[2] == 'p'
                    && buffer[3] == 'i'
                    && buffer[4] == 'r'
                    && buffer[5] == 'e'
                    && buffer[6] == 's'
                ) {
                    return HttpHeaderName.EXPIRES;
                }
                break;
        }
        return null;
    }

    private HttpHeaderName parseI() {
        if (buffer[1] == 'f' && buffer[2] == '-') {
            if (buffer[3] == 'm') {
                if (buffer[4] == 'a' &&
                    buffer[5] == 't' &&
                    buffer[6] == 'c' &&
                    buffer[7] == 'h')
                    return HttpHeaderName.IF_MATCH;

                if (buffer[4] == 'o' &&
                    buffer[5] == 'd' &&
                    buffer[6] == 'i' &&
                    buffer[7] == 'f' &&
                    buffer[8] == 'i' &&
                    buffer[9] == 'e' &&
                    buffer[10] == 'd' &&
                    buffer[11] == '-' &&
                    buffer[12] == 's' &&
                    buffer[13] == 'i' &&
                    buffer[14] == 'n' &&
                    buffer[15] == 'c' &&
                    buffer[16] == 'e'
                ) return HttpHeaderName.IF_MODIFIED_SINCE;
            }
            if (buffer[3] == 'n' &&
                buffer[4] == 'o' &&
                buffer[5] == 'n' &&
                buffer[6] == 'e' &&
                buffer[7] == '-' &&
                buffer[8] == 'm' &&
                buffer[9] == 'a' &&
                buffer[10] == 't' &&
                buffer[11] == 'c' &&
                buffer[12] == 'h')
                return HttpHeaderName.IF_NONE_MATCH;

            if (buffer[3] == 'r' &&
                buffer[4] == 'a' &&
                buffer[5] == 'n' &&
                buffer[6] == 'g' &&
                buffer[7] == 'e')
                return HttpHeaderName.IF_RANGE;

            if (buffer[3] == 'u' &&
                buffer[4] == 'n' &&
                buffer[5] == 'm' &&
                buffer[6] == 'o' &&
                buffer[7] == 'd' &&
                buffer[8] == 'i' &&
                buffer[9] == 'f' &&
                buffer[10] == 'i' &&
                buffer[11] == 'e' &&
                buffer[12] == 'd' &&
                buffer[13] == '-' &&
                buffer[14] == 's' &&
                buffer[15] == 'i' &&
                buffer[16] == 'n' &&
                buffer[17] == 'c' &&
                buffer[18] == 'e')
                return HttpHeaderName.IF_UNMODIFIED_SINCE;
        }
        return null;
    }

    private HttpHeaderName parseP() {
        if (buffer[1] == 'r') {
            if (buffer[2] == 'a' &&
                buffer[3] == 'g' &&
                buffer[4] == 'm' &&
                buffer[5] == 'a'
            ) return HttpHeaderName.PRAGMA;
            if (buffer[2] == 'r' &&
                buffer[3] == 'o' &&
                buffer[4] == 'x' &&
                buffer[5] == 'y' &&
                buffer[6] == '-') { // proxy-
                if (buffer[7] == 'a' &&
                    buffer[8] == 'u' &&
                    buffer[9] == 't' &&
                    buffer[10] == 'h'
                ) { // proxy-auth-
                    if (buffer[11] == 'e' &&
                        buffer[12] == 'n' &&
                        buffer[13] == 't' &&
                        buffer[14] == 'i' &&
                        buffer[15] == 'c' &&
                        buffer[16] == 'a' &&
                        buffer[17] == 't' &&
                        buffer[18] == 'e'
                    ) return HttpHeaderName.PROXY_AUTHENTICATE;
                    if (buffer[11] == 'o' &&
                        buffer[12] == 'r' &&
                        buffer[13] == 'i' &&
                        buffer[14] == 'z' &&
                        buffer[15] == 'a' &&
                        buffer[16] == 't' &&
                        buffer[17] == 'i' &&
                        buffer[18] == 'o' &&
                        buffer[19] == 'n'
                    ) return HttpHeaderName.PROXY_AUTHORIZATION;
                }
                if (buffer[7] == 'c' &&
                    buffer[8] == 'o' &&
                    buffer[9] == 'n' &&
                    buffer[10] == 'n' &&
                    buffer[11] == 'e' &&
                    buffer[12] == 'c' &&
                    buffer[13] == 't' &&
                    buffer[14] == 'i' &&
                    buffer[15] == 'o' &&
                    buffer[16] == 'h'
                ) return HttpHeaderName.PROXY_CONNECTION;
            }
        }
        return null;
    }

    private HttpHeaderName parseR() {
        if (buffer[1] == 'a' &&
            buffer[2] == 'n' &&
            buffer[3] == 'g' &&
            buffer[4] == 'e'
        ) return HttpHeaderName.RANGE;
        if (buffer[1] == 'e') {
            if (buffer[2] == 'f' &&
                buffer[3] == 'e' &&
                buffer[4] == 'r' &&
                buffer[5] == 'e' &&
                buffer[6] == 'r'
            ) return HttpHeaderName.REFERER;
            if (buffer[2] == 't' &&
                buffer[3] == 'r' &&
                buffer[4] == 'y' &&
                buffer[5] == '-' &&
                buffer[6] == 'a' &&
                buffer[7] == 'f' &&
                buffer[8] == 't' &&
                buffer[9] == 'e' &&
                buffer[10] == 'r'
            ) return HttpHeaderName.RETRY_AFTER;
        }
        return null;
    }

    private HttpHeaderName parseS() {
        if (buffer[1] == 'e') {
            if (buffer[2] == 'c' &&
                buffer[3] == '-' &&
                buffer[4] == 'w' &&
                buffer[5] == 'e' &&
                buffer[6] == 'b' &&
                buffer[7] == 's' &&
                buffer[8] == 'o' &&
                buffer[9] == 'c' &&
                buffer[10] == 'k' &&
                buffer[11] == 'e' &&
                buffer[12] == 't' &&
                buffer[13] == '-'
            ) { // sec-websocket-
                if (buffer[14] == 'k' &&
                    buffer[15] == 'e' &&
                    buffer[16] == 'y') {
                    if (pos == 17) {
                        return HttpHeaderName.SEC_WEBSOCKET_KEY;
                    }
                    if (buffer[17] == '1')
                        return HttpHeaderName.SEC_WEBSOCKET_KEY1;
                    if (buffer[17] == '2')
                        return HttpHeaderName.SEC_WEBSOCKET_KEY2;
                }
                if (buffer[14] == 'l' &&
                    buffer[15] == 'o' &&
                    buffer[16] == 'c' &&
                    buffer[17] == 'a' &&
                    buffer[18] == 't' &&
                    buffer[19] == 'i' &&
                    buffer[20] == 'o' &&
                    buffer[21] == 'n'
                ) return HttpHeaderName.SEC_WEBSOCKET_LOCATION;
                if (buffer[14] == 'o' &&
                    buffer[15] == 'r' &&
                    buffer[16] == 'i' &&
                    buffer[17] == 'g' &&
                    buffer[18] == 'i' &&
                    buffer[19] == 'n'
                ) return HttpHeaderName.SEC_WEBSOCKET_ORIGIN;
                if (buffer[14] == 'p' &&
                    buffer[15] == 'r' &&
                    buffer[16] == 'o' &&
                    buffer[17] == 't' &&
                    buffer[18] == 'o' &&
                    buffer[19] == 'c' &&
                    buffer[20] == 'o' &&
                    buffer[21] == 'l'
                ) return HttpHeaderName.SEC_WEBSOCKET_PROTOCOL;
                if (buffer[14] == 'v' &&
                    buffer[15] == 'e' &&
                    buffer[16] == 'r' &&
                    buffer[17] == 's' &&
                    buffer[18] == 'i' &&
                    buffer[19] == 'o' &&
                    buffer[20] == 'n'
                ) return HttpHeaderName.SEC_WEBSOCKET_VERSION;
                if (buffer[14] == 'a' &&
                    buffer[15] == 'c' &&
                    buffer[16] == 'c' &&
                    buffer[17] == 'e' &&
                    buffer[18] == 'p' &&
                    buffer[19] == 't'
                ) return HttpHeaderName.SEC_WEBSOCKET_ACCEPT;
                if (buffer[14] == 'e' &&
                    buffer[15] == 'x' &&
                    buffer[16] == 't' &&
                    buffer[17] == 'e' &&
                    buffer[18] == 'n' &&
                    buffer[19] == 's' &&
                    buffer[20] == 'i' &&
                    buffer[21] == 'o' &&
                    buffer[22] == 'n' &&
                    buffer[23] == 's'
                ) return HttpHeaderName.SEC_WEBSOCKET_EXTENSIONS;

                return null;
            }
            if (buffer[2] == 'r' &&
                buffer[3] == 'v' &&
                buffer[4] == 'e' &&
                buffer[5] == 'r'
            ) return HttpHeaderName.SERVER;
            if (buffer[2] == 't' &&
                buffer[3] == '-' &&
                buffer[4] == 'c' &&
                buffer[5] == 'o' &&
                buffer[6] == 'o' &&
                buffer[7] == 'k' &&
                buffer[8] == 'i' &&
                buffer[9] == 'e'
            ) {
                if (pos == 10) return HttpHeaderName.SET_COOKIE;
                if (buffer[10] == '2') return HttpHeaderName.SET_COOKIE2;
            }
        }
        return null;
    }

    private HttpHeaderName parseT() {
        if (buffer[1] == 'e' && pos == 2) {
            return HttpHeaderName.TE;
        }
        if (buffer[2] == 'r' &&
            buffer[3] == 'a'
        ) {
            if (buffer[4] == 'i' &&
                buffer[5] == 'l' &&
                buffer[6] == 'e' &&
                buffer[7] == 'r'
            ) return HttpHeaderName.TRAILER;
            if (buffer[4] == 'n' &&
                buffer[5] == 's' &&
                buffer[6] == 'f' &&
                buffer[7] == 'e' &&
                buffer[8] == 'r' &&
                buffer[9] == '-' &&
                buffer[10] == 'e' &&
                buffer[11] == 'n' &&
                buffer[12] == 'c' &&
                buffer[13] == 'o' &&
                buffer[14] == 'd' &&
                buffer[15] == 'i' &&
                buffer[16] == 'n' &&
                buffer[17] == 'g'
            ) return HttpHeaderName.TRANSFER_ENCODING;
        }
        return null;
    }

    private HttpHeaderName parseU() {
        if (buffer[1] == 'p' &&
            buffer[2] == 'g' &&
            buffer[3] == 'r' &&
            buffer[4] == 'a' &&
            buffer[5] == 'd' &&
            buffer[6] == 'e'
        ) return HttpHeaderName.UPGRADE;
        if (buffer[1] == 's' &&
            buffer[2] == 'e' &&
            buffer[3] == 'r' &&
            buffer[4] == '-' &&
            buffer[5] == 'a' &&
            buffer[6] == 'g' &&
            buffer[7] == 'e' &&
            buffer[8] == 'n' &&
            buffer[9] == 't'
        ) return HttpHeaderName.USER_AGENT;
        return null;
    }

    private HttpHeaderName parseV() {
        if (buffer[1] == 'a' &&
            buffer[2] == 'r' &&
            buffer[3] == 'y'
        ) return HttpHeaderName.VARY;
        if (buffer[1] == 'i' &&
            buffer[2] == 'a'
        ) return HttpHeaderName.VIA;
        return null;
    }

    private HttpHeaderName parseW() {
        if (buffer[1] == 'a' &&
            buffer[2] == 'r' &&
            buffer[3] == 'n' &&
            buffer[4] == 'i' &&
            buffer[5] == 'n' &&
            buffer[6] == 'g'
        ) return HttpHeaderName.WARNING;
        if (buffer[1] == 'e' &&
            buffer[2] == 'b' &&
            buffer[3] == 's' &&
            buffer[4] == 'o' &&
            buffer[5] == 'c' &&
            buffer[6] == 'k' &&
            buffer[7] == 'e' &&
            buffer[8] == 't' &&
            buffer[9] == '-'
        ) {
            if (buffer[10] == 'l' &&
                buffer[11] == 'o' &&
                buffer[12] == 'c' &&
                buffer[13] == 'a' &&
                buffer[14] == 't' &&
                buffer[15] == 'i' &&
                buffer[16] == 'o' &&
                buffer[17] == 'n'
            ) return HttpHeaderName.WEBSOCKET_LOCATION;
            if (buffer[10] == 'o' &&
                buffer[11] == 'r' &&
                buffer[12] == 'i' &&
                buffer[13] == 'g' &&
                buffer[14] == 'i' &&
                buffer[15] == 'n'
            ) return HttpHeaderName.WEBSOCKET_ORIGIN;
            if (buffer[10] == 'p' &&
                buffer[11] == 'r' &&
                buffer[12] == 'o' &&
                buffer[13] == 't' &&
                buffer[14] == 'o' &&
                buffer[15] == 'c' &&
                buffer[16] == 'o' &&
                buffer[17] == 'l'
            ) return HttpHeaderName.WEBSOCKET_PROTOCOL;
            return null;
        }
        if (buffer[1] == 'w' &&
            buffer[2] == 'w' &&
            buffer[3] == '-' &&
            buffer[4] == 'a' &&
            buffer[5] == 'u' &&
            buffer[6] == 't' &&
            buffer[7] == 'h' &&
            buffer[8] == 'e' &&
            buffer[9] == 'n' &&
            buffer[10] == 't' &&
            buffer[11] == 'i' &&
            buffer[12] == 'c' &&
            buffer[13] == 'a' &&
            buffer[14] == 't' &&
            buffer[15] == 'e'
        ) return HttpHeaderName.WWW_AUTHENTICATE;
        return null;
    }

    private HttpHeaderName parseX() {
        if (buffer[1] == '-' &&
            buffer[2] == 'f' &&
            buffer[3] == 'r' &&
            buffer[4] == 'a' &&
            buffer[5] == 'm' &&
            buffer[6] == 'e' &&
            buffer[7] == '-' &&
            buffer[8] == 'o' &&
            buffer[9] == 'p' &&
            buffer[10] == 't' &&
            buffer[11] == 'i' &&
            buffer[12] == 'o' &&
            buffer[13] == 'n' &&
            buffer[14] == 's'
        ) return HttpHeaderName.WARNING;
        return null;
    }
}
