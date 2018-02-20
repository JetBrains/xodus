# <img src="https://raw.githubusercontent.com/wiki/jetbrains/xodus/xodus.png" width=160>

<!--
[Overview](https://github.com/JetBrains/xodus/Database-Encryption#overview)
<br>[Working with Encrypted Database](https://github.com/JetBrains/xodus/Database-Encryption#working-with-encrypted-database)
<br>[Encrypting Existing Database](https://github.com/JetBrains/xodus/Database-Encryption#encrypting-existing-database)
<br>[Using Custom Cipher Implementations](https://github.com/JetBrains/xodus/Database-Encryption#using-custom-cipher-implementations)
-->

## Overview

As of version 1.2.0 Xodus supports database encryption. Implementations of algorithms are pluggable, but only
symmetric [stream ciphers](https://en.wikipedia.org/wiki/Stream_cipher) can be used.

Xodus supports two algorithms out-of-the-box: [Salsa20](https://en.wikipedia.org/wiki/Salsa20) and
[ChaCha20](https://en.wikipedia.org/wiki/Salsa20#ChaCha_variant). Salsa20 is one of two the best  
algorithms (along with [Rabbit](https://en.wikipedia.org/wiki/Rabbit_(cipher))) submitted to the
[eSTREAM](https://en.wikipedia.org/wiki/ESTREAM) project in "profile 1" (<i>Stream ciphers for
software applications with high throughput requirements</i>).

[ChaCha20](https://en.wikipedia.org/wiki/Salsa20#ChaCha_variant) is an "extension" of Salsa20 which
tends to be even more secure while achieving the same or slightly better performance. ChaCha20 is widely
being adopted. Google has selected ChaCha20 as a replacement for [RC4](https://en.wikipedia.org/wiki/RC4)
in [TLS](https://en.wikipedia.org/wiki/Transport_Layer_Security). ChaCha20 is used in
[OpenSSH](https://en.wikipedia.org/wiki/OpenSSH).

Both built-in implementations (Salsa20 and ChaCha20) are provided by the
[Legion of the Bouncy Castle](https://www.bouncycastle.org). 

## Working with Encrypted Database

If you are ok about using built-in Salsa20 or ChaCha20, then depend your application of the
[xodus-crypto jar](https://search.maven.org/#search%7Cga%7C1%7Cxodus-crypto). Otherwise you have to
provide your own implementation (see [Using Custom Encryption Algorithms](https://github.com/JetBrains/xodus/Database-Encryption#using-custom-encryption-algorithms)).
When opening/creating a database your application should configure <i>cipher id</i>, <i>cipher key</i> and
<i>cipher basic IV</i> ([initialization vector](https://en.wikipedia.org/wiki/Initialization_vector)).
Opening a database can look like the following:

```java
final EnvironmentConfig config = new EnvironmentConfig();
config.setCipherId("jetbrains.exodus.crypto.streamciphers.ChaChaStreamCipherProvider");
config.setCipherKey("000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f");
config.setCipherBasicIV(314159262718281828);
try (Environment environment = Environments.newInstance("/Users/me/.myAppData", config)) {
    // ...
}
```

Cipher id, cipher key and cipher basic IV can also be passed to the application using corresponding
system properties: `exodus.cipherId`, `exodus.cipherKey` and `exodus.cipherBasicIV`.

Id of the cipher defines stream cipher type (algorithm). Id is an arbitrary string,
but it's recommended to use a fully qualified name of the
[StreamCipherProvider](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/crypto/StreamCipherProvider.java)
implementation. `StreamCipherProvider` is used internally to create instances of
[StreamCipher](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/crypto/StreamCipher.java).

`StreamCipher` instances are initialized with cipher key and cipher basic IV. The key is expected to be a hex string
representing a byte array which is passed to `StreamCipher.init(byte[], long)`. The length of the key
depends on the algorithm (cipher id). Salsa20 can accept both 128-bit and 256-bits keys, whereas
ChaCha20 accepts only 256-bit keys. Basic IV is expected to be random (pseudo-random) and unique
long value. Basic IV is used to calculate relative IVs which are passed to
`StreamCipher.init(byte[], long)`.

All cipher parameters cannot be changed during the life of the database. If your application opens
plain (not encrypted) database with some cipher parameters, or if it opens encrypted database without
cipher parameters, or if it opens encrypted database with different cipher parameters, then
`InvalidCipherParametersException` will be thrown not affecting the database.
So handle it to your own good.

## Encrypting Existing Database

You can encrypt/decrypt existing database using the `Scytale` tool. Download the
[xodus-tools jar](https://search.maven.org/#search%7Cga%7C1%7Cxodus-tools) of the version 1.2.0 or higher
and run:

    ./java -jar xodus-tools.jar scytale
    
You will get the usage message which is quite self-explaining:

```text
Usage: Scytale [options] source target key basicIV [cipher]
Source can be archive or folder
Cipher can be 'Salsa' or 'ChaCha', 'ChaCha' is default
Options:
  -g              use gzip compression when opening archive
  -z              make target an archive
  -o              overwrite target archive or folder
```

E.g., to encrypt the database located at `/Users/me/.myAppData` using ChaCha20 cipher and the same parameters
as in the sample above, run:

        ./java -jar xodus-tools.jar scytale -o /Users/me/.myAppData /Users/me/.myAppData 000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f 314159262718281828 

## Using Custom Cipher Implementations