# License generation for SEP

The licensing process for SEP consists of:
1. Generating a key pair (this should only be done once):
    ```
    ./bin/generate-product-keys.sh <private key file path>
    ```
    * __Keep the private key safe and secure__
    * The script puts the public key in the repository, to be distributed in the SEP JAR
2. Use the private key to sign a JSON license file:
    ```
    ./bin/sign-product-license.sh <private key file path> <unsigned license file path> <signed license file path>
    ```
    An example of an unsigned license file:
    ```
    {
      "owner": "Marie-hélène de Mendoza <mdemendoza@test.starburstdata.net>",
      "expiry": "2525-01-01T00:00:00",
      "features": [
        "ranger",
        "sentry",
        "somePlugin.someFeature",
        ...
      ]
    }
    ```
    The signed license file additionally contains a `base64Signature` attribute.
3. Send the signed license to the client.
4. Client stores signed license in a `etc/starburstdata.license` path on the coordinator node.

It's important to note that __once a public key is distributed to clients, it shouldn't be changed unless the corresponding private key has been compromised__.
Changing the keys requires re-signing of all issued licenses and sending them to clients when they update SEP, since the new public key won't validate old licenses.

### Licenses for internal testing

There are two other scripts in the `bin` directory which generate test keys and sign test licenses used during starburst-license unit testing.
These keys and licenses won't work with real SEP servers.

For purposes of end-to-end testing, we need to have some licenses signed with the real private key.
These licenses would work with real SEP servers, so we decided to distribute them in a separate repository.
They could be used to enable features without paying for them, so it's important they are never made public.
