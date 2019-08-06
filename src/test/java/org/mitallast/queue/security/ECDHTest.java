package org.mitallast.queue.security;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;

import java.math.BigInteger;
import java.util.Random;


public class ECDHTest extends BaseTest {

//    @Test
//    public void testFlow() throws Exception {
//        Config config = ConfigFactory.defaultReference();
//        SecurityService securityService = new SecurityService(config);
//        ECDHFlow alice = securityService.ecdh();
//        ECDHFlow bob = securityService.ecdh();
//
//        bob.keyAgreement(alice.requestStart());
//        alice.keyAgreement(bob.responseStart());
//
//        byte[] source = randomBytes(1024);
//        ECDHEncrypted encrypted = bob.encrypt(source);
//
//        byte[] decrypted = alice.decrypt(encrypted);
//        Assert.assertArrayEquals(source, decrypted);
//        decrypted = alice.decrypt(encrypted);
//        Assert.assertArrayEquals(source, decrypted);
//        decrypted = alice.decrypt(encrypted);
//        Assert.assertArrayEquals(source, decrypted);
//    }

    @Test
    public void benchmarkEncryptRSA() throws Exception {
        Config config = ConfigFactory.defaultReference();
        SecurityService securityService = new SecurityService(config);

        byte[] data = randomBytes(200);
        int total = 10000;
        long start = System.currentTimeMillis();
        byte[] encrypted = randomBytes(1);
        for (int i = 0; i < total; i++) {
            encrypted = securityService.encrypt(data);
        }
        long end = System.currentTimeMillis();
        printQps("encrypt", total, start, end);

        total = 1000;
        start = System.currentTimeMillis();
        for (int i = 0; i < total; i++) {
            byte[] decrypted = securityService.decrypt(encrypted);
            assert decrypted.length == data.length;
        }
        end = System.currentTimeMillis();
        printQps("decrypt", total, start, end);
    }

//    @Test
//    public void benchmarkEncryptAES() throws Exception {
//        Config config = ConfigFactory.defaultReference();
//        SecurityService securityService = new SecurityService(config);
//        ECDHFlow alice = securityService.ecdh();
//        ECDHFlow bob = securityService.ecdh();
//
//        bob.keyAgreement(alice.requestStart());
//        alice.keyAgreement(bob.responseStart());
//
//        byte[] source = randomBytes(256);
//        ECDHEncrypted encrypted = bob.encrypt(source);
//        int total = 100000;
//        long start = System.currentTimeMillis();
//        for (int i = 0; i < total; i++) {
//            encrypted = bob.encrypt(source);
//        }
//        long end = System.currentTimeMillis();
//        printQps("encrypt", total, start, end);
//
//        start = System.currentTimeMillis();
//        for (int i = 0; i < total; i++) {
//            byte[] decrypted = alice.decrypt(encrypted);
//            assert decrypted.length == source.length;
//        }
//        end = System.currentTimeMillis();
//        printQps("decrypt", total, start, end);
//    }

    @Test
    public void benchmarkSignRSA() throws Exception {
        Config config = ConfigFactory.defaultReference();
        SecurityService securityService = new SecurityService(config);

        byte[] data = randomBytes(1024);
        byte[] sign = randomBytes(1);

        int total = 100;
        long start = System.currentTimeMillis();
        for (int i = 0; i < total; i++) {
            sign = securityService.sign(data);
        }
        long end = System.currentTimeMillis();
        printQps("sign", total, start, end);

        total = 10000;
        start = System.currentTimeMillis();
        for (int i = 0; i < total; i++) {
            assert securityService.verify(data, sign);
        }
        end = System.currentTimeMillis();
        printQps("verify", total, start, end);
    }

//    @Test
//    public void benchmarkSignHMac() throws Exception {
//        Config config = ConfigFactory.defaultReference();
//        SecurityService securityService = new SecurityService(config);
//        ECDHFlow alice = securityService.ecdh();
//        ECDHFlow bob = securityService.ecdh();
//
//        bob.keyAgreement(alice.requestStart());
//        alice.keyAgreement(bob.responseStart());
//
//        byte[] iv = randomBytes(256);
//        byte[] data = randomBytes(256);
//        byte[] sign = randomBytes(1);
//
//        for (int k = 0; k < 3; k++) {
//            int total = 1000000;
//            long start = System.currentTimeMillis();
//            for (int i = 0; i < total; i++) {
//                sign = alice.sign(iv, data);
//            }
//            long end = System.currentTimeMillis();
//            printQps("sign", total, start, end);
//        }
//
//        for (int k = 0; k < 3; k++) {
//            int total = 1000000;
//            long start = System.currentTimeMillis();
//            for (int i = 0; i < total; i++) {
//                assert bob.verify(iv, data, sign);
//            }
//            long end = System.currentTimeMillis();
//            printQps("verify", total, start, end);
//        }
//    }
}
