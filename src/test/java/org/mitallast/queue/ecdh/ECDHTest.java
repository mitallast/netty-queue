package org.mitallast.queue.ecdh;

import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;

public class ECDHTest extends BaseTest {
    @Test
    public void testFlow() throws Exception {
        ECDHFlow alice = new ECDHFlow();
        ECDHFlow bob = new ECDHFlow();

        byte[] start = alice.publicKey();

        bob.keyAgreement(start);
        alice.keyAgreement(bob.publicKey());

        byte[] source = randomBytes(2 << 10);
        Encrypted encrypted = bob.encrypt(source);
        byte[] decrypted = alice.decrypt(encrypted);

        Assert.assertArrayEquals(source, decrypted);
    }
}
