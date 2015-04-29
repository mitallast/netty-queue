package org.mitallast.queue.rest.action.support;

import org.mitallast.queue.QueueParseException;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.common.xstream.XStreamParser;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageType;

import java.io.IOException;

public class QueueMessageParser {

    public static void parse(QueueMessage queueMessage, XStreamParser parser) throws IOException {
        String currentFieldName;
        XStreamParser.Token token;

        token = parser.nextToken();
        if (token == null) {
            throw new QueueParseException("malformed, expected settings to start with 'object', actual [null]");
        }
        if (token != XStreamParser.Token.START_OBJECT) {
            throw new QueueParseException("malformed, expected settings to start with 'object', actual [" + token + "]");
        }

        while ((token = parser.nextToken()) != XStreamParser.Token.END_OBJECT) {
            if (token == XStreamParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                switch (currentFieldName) {
                    case "message":
                        token = parser.nextToken();
                        if (token == XStreamParser.Token.VALUE_STRING) {
                            queueMessage.setSource(parser.text());
                        } else if (token == XStreamParser.Token.START_OBJECT || token == XStreamParser.Token.START_ARRAY) {
                            queueMessage.setSource(QueueMessageType.JSON, parser.rawBytes());
                        } else {
                            throw new QueueParseException("malformed, expected string, object or array value at field [" + currentFieldName + "]");
                        }
                        break;
                    case "uuid":
                        token = parser.nextToken();
                        if (token == XStreamParser.Token.VALUE_STRING) {
                            queueMessage.setUuid(UUIDs.fromString(parser.text()));
                        } else {
                            throw new QueueParseException("malformed, expected string value at field [" + currentFieldName + "]");
                        }
                        break;
                    default:
                        throw new QueueParseException("malformed, unexpected field [" + currentFieldName + "]");
                }
            }
        }
    }
}
