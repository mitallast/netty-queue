package org.mitallast.queue.security;

import io.netty.util.AttributeKey;
import org.conscrypt.OpenSSLProvider;

import javax.crypto.Cipher;
import javax.crypto.KeyAgreement;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.security.*;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@SuppressWarnings("unchecked")
public class ECDHFlow {

    public static final AttributeKey<ECDHFlow> key = AttributeKey.valueOf("ECDH");

    static {
        String errorString = "Failed manually overriding key-length permissions.";
        int newMaxKeyLength;
        try {
            if ((newMaxKeyLength = Cipher.getMaxAllowedKeyLength("AES")) < 256) {
                Class c = Class.forName("javax.crypto.CryptoAllPermissionCollection");
                Constructor con = c.getDeclaredConstructor();
                con.setAccessible(true);
                Object allPermissionCollection = con.newInstance();
                Field f = c.getDeclaredField("all_allowed");
                f.setAccessible(true);
                f.setBoolean(allPermissionCollection, true);

                c = Class.forName("javax.crypto.CryptoPermissions");

                con = c.getDeclaredConstructor();
                con.setAccessible(true);
                Object allPermissions = con.newInstance();
                f = c.getDeclaredField("perms");
                f.setAccessible(true);

                ((Map) f.get(allPermissions)).put("*", allPermissionCollection);

                c = Class.forName("javax.crypto.JceSecurityManager");
                f = c.getDeclaredField("defaultPolicy");
                f.setAccessible(true);
                Field mf = Field.class.getDeclaredField("modifiers");
                mf.setAccessible(true);
                mf.setInt(f, f.getModifiers() & ~Modifier.FINAL);
                f.set(null, allPermissions);

                newMaxKeyLength = Cipher.getMaxAllowedKeyLength("AES");
            }
        } catch (Exception e) {
            throw new RuntimeException(errorString, e);
        }
        if (newMaxKeyLength < 256)
            throw new RuntimeException(errorString); // hack failed
    }

    private static final String PROVIDER = "GC";
    private static final String ECC_KEY_TYPE = "EC";
    private static final String ECC_CURVE = "secp224r1";
    private static final String ECC_SIGNATURE = "SHA1withECDSA";
    private static final String ECDH_AGREEMENT = "ECDH";
    private static final String AES256 = "AES/CBC/PKCS5Padding";
    private static final String DIGEST = "SHA-256";

    static {
        Security.addProvider(new OpenSSLProvider(PROVIDER));
    }

    private enum State {START, AGREEMENT}

    private final KeyPair keyPair;
    private State state;
    private PublicKey otherPublicKey;
    private SecretKey secretKey;

    private final SecurityService securityService;
    private final Signature signSignature;
    private final Signature verifySignature;
    private final Cipher encryptCipher;
    private final Cipher decryptCipher;
    private final CompletableFuture<Void> agreementFuture;

    public ECDHFlow(SecurityService securityService) throws GeneralSecurityException {
        this.securityService = securityService;
        this.signSignature = Signature.getInstance(ECC_SIGNATURE, PROVIDER);
        this.verifySignature = Signature.getInstance(ECC_SIGNATURE, PROVIDER);
        this.encryptCipher = Cipher.getInstance(AES256);
        this.decryptCipher = Cipher.getInstance(AES256);
        this.agreementFuture = new CompletableFuture<>();

        KeyPairGenerator generator = KeyPairGenerator.getInstance(ECC_KEY_TYPE, PROVIDER);
        ECGenParameterSpec spec = new ECGenParameterSpec(ECC_CURVE);
        generator.initialize(spec);
        keyPair = generator.genKeyPair();

        state = State.START;
    }

    public ECDHRequest requestStart() throws GeneralSecurityException {
        byte[] publicKey = keyPair.getPublic().getEncoded();

        byte[] sign = securityService.sign(publicKey);
        byte[] encrypted = securityService.encrypt(publicKey);
        return new ECDHRequest(sign, encrypted);
    }

    public ECDHResponse responseStart() throws GeneralSecurityException {
        byte[] publicKey = keyPair.getPublic().getEncoded();

        byte[] sign = securityService.sign(publicKey);
        byte[] encrypted = securityService.encrypt(publicKey);
        return new ECDHResponse(sign, encrypted);
    }

    public void keyAgreement(ECDHRequest start) throws Exception {
        byte[] decrypted = securityService.decrypt(start.encodedKey());
        if (!securityService.verify(decrypted, start.sign())) {
            throw new IllegalArgumentException("not verified");
        }
        keyAgreement(decrypted);
    }

    public void keyAgreement(ECDHResponse start) throws Exception {
        byte[] decrypted = securityService.decrypt(start.encodedKey());
        if (!securityService.verify(decrypted, start.sign())) {
            throw new IllegalArgumentException("not verified");
        }
        keyAgreement(decrypted);
    }

    public synchronized void keyAgreement(byte[] publicKey) throws Exception {
        if (state != State.START) {
            throw new IllegalStateException("dh not in started state");
        }
        this.state = State.AGREEMENT;
        KeyFactory kf = KeyFactory.getInstance(ECC_KEY_TYPE, PROVIDER);
        X509EncodedKeySpec pkSpec = new X509EncodedKeySpec(publicKey);
        otherPublicKey = kf.generatePublic(pkSpec);

        // Perform key agreement
        KeyAgreement ka = KeyAgreement.getInstance(ECDH_AGREEMENT);
        ka.init(keyPair.getPrivate());
        ka.doPhase(otherPublicKey, true);
        byte[] sharedSecret = ka.generateSecret();

        // Derive a key from the shared secret and both public keys
        MessageDigest hash = MessageDigest.getInstance(DIGEST);
        hash.update(sharedSecret);
        // Simple deterministic ordering
        List<ByteBuffer> keys = Arrays.asList(
                ByteBuffer.wrap(keyPair.getPublic().getEncoded()),
                ByteBuffer.wrap(publicKey));
        Collections.sort(keys);
        hash.update(keys.get(0));
        hash.update(keys.get(1));

        byte[] derivedKey = hash.digest();
        secretKey = new SecretKeySpec(derivedKey, "AES");

        agreementFuture.complete(null);
    }

    public byte[] sign(byte[] data) throws Exception {
        signSignature.initSign(keyPair.getPrivate());
        signSignature.update(data);
        return signSignature.sign();
    }

    public boolean verify(byte[] data, byte[] sign) throws Exception {
        verifySignature.initVerify(otherPublicKey);
        verifySignature.update(data);
        return verifySignature.verify(sign);
    }

    public ECDHEncrypted encrypt(byte[] data) throws Exception {
        if (state != State.AGREEMENT) {
            throw new IllegalStateException("no shared key");
        }

        encryptCipher.init(Cipher.ENCRYPT_MODE, secretKey);
        AlgorithmParameters params = encryptCipher.getParameters();
        byte[] iv = params.getParameterSpec(IvParameterSpec.class).getIV();
        byte[] encrypted = encryptCipher.doFinal(data);
        byte[] sign = sign(data);
        return new ECDHEncrypted(sign, iv, encrypted);
    }

    public byte[] decrypt(ECDHEncrypted encrypted) throws Exception {
        if (state != State.AGREEMENT) {
            throw new IllegalStateException("no shared key");
        }

        decryptCipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(encrypted.iv()));
        byte[] decrypted = decryptCipher.doFinal(encrypted.encrypted());
        if (!verify(decrypted, encrypted.sign())) {
            throw new IllegalArgumentException("not verified");
        }
        return decrypted;
    }

    public boolean isAgreement() {
        return state == State.AGREEMENT;
    }

    public CompletableFuture<Void> agreementFuture() {
        return agreementFuture;
    }
}
