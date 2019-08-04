package org.mitallast.queue.security;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.vavr.collection.List;
import org.conscrypt.OpenSSLProvider;
import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.*;
import java.security.spec.ECGenParameterSpec;

//import static javax.xml.bind.DatatypeConverter.printHexBinary;

public class NativeCryptoTest extends BaseTest {
    static {
        Security.addProvider(new OpenSSLProvider("BS"));
    }

    @Test
    public void testGenerateRSA() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA", "BS");
        generator.initialize(2048);
        KeyPair keyPair = generator.genKeyPair();

        System.out.println("private");
        System.out.println(keyPair.getPrivate().getFormat());
//        System.out.println(printHexBinary(keyPair.getPrivate().getEncoded()));

        System.out.println("public");
        System.out.println(keyPair.getPublic().getFormat());
//        System.out.println(printHexBinary(keyPair.getPublic().getEncoded()));
    }

    @Test
    public void testSignRSA() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA", "BS");
        generator.initialize(2048);
        KeyPair keyPair = generator.genKeyPair();

        byte[] bytes = randomBytes(1024);

        Signature signSignature = Signature.getInstance("SHA1withRSA", "BS");
        signSignature.initSign(keyPair.getPrivate());
        signSignature.update(bytes);
        byte[] sign = signSignature.sign();

        Signature verifySignature = Signature.getInstance("SHA1withRSA", "BS");
        verifySignature.initVerify(keyPair.getPublic());
        verifySignature.update(bytes);
        assert verifySignature.verify(sign);
    }

    @Test
    public void benchmarkSignRSA() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA", "BS");
        generator.initialize(2048);
        KeyPair keyPair = generator.genKeyPair();

        byte[] bytes = randomBytes(1024);

        int total = 1000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < total; i++) {
            Signature signSignature = Signature.getInstance("SHA1withRSA", "BS");
            signSignature.initSign(keyPair.getPrivate());
            signSignature.update(bytes);
            signSignature.sign();
        }
        long end = System.currentTimeMillis();
        printQps("sign SHA1withRSA (2048)", total, start, end);
    }

    @Test
    public void benchmarkVerifyRSA() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA", "BS");
        generator.initialize(2048);
        KeyPair keyPair = generator.genKeyPair();

        byte[] bytes = randomBytes(1024);
        Signature signSignature = Signature.getInstance("SHA1withRSA", "BS");
        signSignature.initSign(keyPair.getPrivate());
        signSignature.update(bytes);
        byte[] sign = signSignature.sign();

        int total = 1000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < total; i++) {
            Signature verifySignature = Signature.getInstance("SHA1withRSA", "BS");
            verifySignature.initVerify(keyPair.getPublic());
            verifySignature.update(bytes);
            assert verifySignature.verify(sign);
        }
        long end = System.currentTimeMillis();
        printQps("sign SHA1withRSA (2048)", total, start, end);
    }

    @Test
    public void testGenerateEC() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC", "BS");
        ECGenParameterSpec spec = new ECGenParameterSpec("prime256v1");
        generator.initialize(spec);
        KeyPair keyPair = generator.genKeyPair();
    }

    @Test
    public void testSignECDSA() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC", "BS");
        ECGenParameterSpec spec = new ECGenParameterSpec("prime256v1");
        generator.initialize(spec);
        KeyPair keyPair = generator.genKeyPair();

        byte[] bytes = randomBytes(1024);

        Signature signSignature = Signature.getInstance("SHA1withECDSA", "BS");
        signSignature.initSign(keyPair.getPrivate());
        signSignature.update(bytes);
        byte[] sign = signSignature.sign();

        Signature verifySignature = Signature.getInstance("SHA1withECDSA", "BS");
        verifySignature.initVerify(keyPair.getPublic());
        verifySignature.update(bytes);
        assert verifySignature.verify(sign);
    }

    @Test
    public void benchmarkSignECDSA() throws Exception {
        List<String> curves = List.of("secp224r1", "prime256v1", "secp384r1", "secp521r1");
        List<String> signatures = List.of("SHA1withECDSA", "SHA224withECDSA", "SHA384withECDSA", "SHA512withECDSA");

        for (String signature : signatures) {
            for (String curve : curves) {

                KeyPairGenerator generator = KeyPairGenerator.getInstance("EC", "BS");
                ECGenParameterSpec spec = new ECGenParameterSpec(curve);
                generator.initialize(spec);
                KeyPair keyPair = generator.genKeyPair();

                byte[] bytes = randomBytes(1024);

                int total = 50000;
                long start = System.currentTimeMillis();
                for (int i = 0; i < total; i++) {
                    Signature signSignature = Signature.getInstance(signature, "BS");
                    signSignature.initSign(keyPair.getPrivate());
                    signSignature.update(bytes);
                    signSignature.sign();
                }
                long end = System.currentTimeMillis();
                printQps("sign " + signature + " (" + curve + ")", total, start, end);
            }
        }
    }

    @Test
    public void benchmarkVerifyECDSA() throws Exception {
        List<String> curves = List.of("secp224r1", "prime256v1", "secp384r1", "secp521r1");
        List<String> signatures = List.of("SHA1withECDSA", "SHA224withECDSA", "SHA384withECDSA", "SHA512withECDSA");

        for (String signature : signatures) {
            for (String curve : curves) {
                KeyPairGenerator generator = KeyPairGenerator.getInstance("EC", "BS");
                ECGenParameterSpec spec = new ECGenParameterSpec(curve);
                generator.initialize(spec);
                KeyPair keyPair = generator.genKeyPair();

                byte[] data = randomBytes(1024);
                Signature signSignature = Signature.getInstance(signature, "BS");
                signSignature.initSign(keyPair.getPrivate());
                signSignature.update(data);
                byte[] sign = signSignature.sign();

                int total = 10000;
                long start = System.currentTimeMillis();
                for (int i = 0; i < total; i++) {
                    Signature verifySignature = Signature.getInstance(signature, "BS");
                    verifySignature.initVerify(keyPair.getPublic());
                    verifySignature.update(data);
                    assert verifySignature.verify(sign);
                }
                long end = System.currentTimeMillis();
                printQps("verify " + signature + " (" + curve + ")", total, start, end);
            }
        }
    }

    @Test
    public void testEncryptECC() throws Exception {
        Config config = ConfigFactory.defaultReference();
        ECDHFlow ecdh = new SecurityService(config).ecdh();

        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC", "BS");
        ECGenParameterSpec spec = new ECGenParameterSpec("prime256v1");
        generator.initialize(spec);
        KeyPair keyPair = generator.genKeyPair();

        byte[] key = randomBytes(32);
        byte[] data = randomBytes(1024);


        SecretKey secret = new SecretKeySpec(key, "AES");
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");

        cipher.init(Cipher.ENCRYPT_MODE, secret);
        AlgorithmParameters params = cipher.getParameters();
        byte[] iv = params.getParameterSpec(IvParameterSpec.class).getIV();
        byte[] encrypted = cipher.doFinal(data);

        cipher.init(Cipher.DECRYPT_MODE, secret, new IvParameterSpec(iv));
        byte[] decrypted = cipher.doFinal(encrypted);

        Assert.assertArrayEquals(data, decrypted);
    }
}
