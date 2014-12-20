package org.mitallast.queue.queue.service;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.queue.QueueMessage;

import java.io.File;
import java.io.IOException;

public class FileQueueMessageAppendTransactionLogTest extends BaseQueueMessageTest {

    private File metaFile;
    private File dataFile;
    private FileQueueMessageAppendTransactionLog transactionLog;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        metaFile = folder.newFile();
        dataFile = folder.newFile();
        transactionLog = new FileQueueMessageAppendTransactionLog(metaFile, dataFile);
        transactionLog.initializeNew();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        transactionLog.close();
    }

    @Test
    public void testPutAndRead() throws IOException {
        QueueMessage newMessage = createMessageWithUuid();
        int pos = transactionLog.putMessage(newMessage);
        assert pos == 0;
        QueueMessage savedMessage = transactionLog.readMessage(newMessage.getUuid());
        assert newMessage.equals(savedMessage) : newMessage + " != " + savedMessage;
    }

    @Test
    public void testPutDeleteAndRead() throws IOException {
        QueueMessage newMessage = createMessageWithUuid();
        transactionLog.putMessage(newMessage);
        transactionLog.markMessageDeleted(newMessage.getUuid());
        QueueMessage savedMessage = transactionLog.readMessage(newMessage.getUuid());
        assert savedMessage == null;
    }

    @Test
    public void testInitializeExists() throws IOException {
        QueueMessage newMessage = createMessageWithUuid();
        transactionLog.putMessage(newMessage);

        try (FileQueueMessageAppendTransactionLog transactionLogExists =
                     new FileQueueMessageAppendTransactionLog(metaFile, dataFile)) {
            transactionLogExists.initializeExists();
            QueueMessage existsMessage = transactionLogExists.readMessage(newMessage.getUuid());
            assert existsMessage != null;
            assert newMessage.equals(existsMessage);
        }
    }

    @Test
    public void testMeta() throws IOException {
        FileQueueMessageAppendTransactionLog.QueueMessageMeta messageMeta =
                new FileQueueMessageAppendTransactionLog.QueueMessageMeta(UUIDs.generateRandom(), 13, 2314, 2341324, 0);
        transactionLog.writeMeta(messageMeta, 0l);
        FileQueueMessageAppendTransactionLog.QueueMessageMeta messageMetaActual = transactionLog.readMeta(0l);
        assert messageMeta.equals(messageMetaActual) : "\n" + messageMeta + " != \n" + messageMetaActual;
    }
}
