package org.sunbird.auth.verifier;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.security.PublicKey;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.common.models.util.PropertiesCache;

@RunWith(PowerMockRunner.class)
@PrepareForTest({PropertiesCache.class})
@PowerMockIgnore({"javax.management.*"})
public class KeyManagerTest {

    @Test
    public void testLoadPublicKey() throws Exception {
        PublicKey key =
                KeyManager.loadPublicKey(
                        "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAysH/wWtg0IjBL1JZZDYvUJC42JCxVobalckr2/3d3eEiWkk7Zh/4DAPYOs4UPjAevTs5VMUjq9EZu/u4H5hNzoVmYNvhtxbhWNY3n4mxpA4Lgt4sNGiGYNNGrN34ML+7+TR3Z1dlrhA271PiuanHI11YymskQRPhBfuwK923Kl/lgI4rS9OQ4GnkvwkUPvMUIRfNt8wL9uTbWm3V9p8VTcmQbW+pPw9QhO9v95NOgXQrLnT8xwnzQE6UCTY2al3B0fc3ULmcxvK+7P1R3/0w1qJLEKSiHl0xnv4WNEfS+2UmN+8jfdSCfoyVIglQl5/tb05j89nfZZp8k24AWLxIJQIDAQAB");
        assertNotNull(key);
    }
}
