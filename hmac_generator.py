import base64
import logging
import os
import hmac

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class HmacSignatureBuilder:
    DELIMITER = b'\n'

    def __init__(self):
        self._algorithm = None
        self._host = None
        self._method = None
        self._resource = None
        self._nonce = None
        self._apiKey = None
        self._apiSecret = None
        self._date = None
        self._contentType = None

    def set_algorithm(self, algorithm):
        self._algorithm = algorithm
        return self

    def set_host(self, host):
        self._host = host
        return self

    def set_apiKey(self, key):
        self._apiKey = key
        return self

    def set_method(self, method):
        self._method = method
        return self

    def set_resource(self, resource):
        self._resource = resource
        return self

    def set_contentType(self, contentType):
        self._contentType = contentType
        return self

    def set_date(self, dateString):
        self._date = dateString
        return self

    def set_nonce(self, nonce):
        self._nonce = nonce
        return self

    def set_apiSecret(self, secret):
        if isinstance(secret, str):
            self._apiSecret = secret.encode('utf-8')
        else:
            self._apiSecret = secret
        return self

    def build(self):
        if not all([self._algorithm, self._host, self._method, self._resource,
                    self._contentType, self._apiKey, self._date]):
            raise ValueError("Missing required fields to build signature")

        digest = hmac.new(self._apiSecret, digestmod=self._algorithm)
        digest.update(self._method.encode('utf-8'))
        digest.update(self.DELIMITER)
        digest.update(self._host.encode('utf-8'))
        digest.update(self.DELIMITER)
        digest.update(self._resource.encode('utf-8'))
        digest.update(self.DELIMITER)
        digest.update(self._contentType.encode('utf-8'))
        digest.update(self.DELIMITER)
        digest.update(self._apiKey.encode('utf-8'))
        digest.update(self.DELIMITER)
        if self._nonce is not None:
            digest.update(self._nonce.encode('utf-8'))
        digest.update(self.DELIMITER)
        digest.update(self._date.encode('utf-8'))
        digest.update(self.DELIMITER)
        digest.update(self.DELIMITER)

        signatureBytes = digest.digest()
        logger.debug("signatureBytes %s", list(signatureBytes))
        return signatureBytes

    def isHashEquals(self, expectedSignature):
        signature = self.build()
        logger.debug("signature : %s", list(signature))
        return hmac.compare_digest(signature, expectedSignature)

    def buildAsHexString(self):
        return self.build().hex().upper()

    def buildAsBase64String(self):
        return base64.b64encode(self.build()).decode('utf-8')


def get_signature(date):
    api_key = os.getenv("API_KEY")
    api_secret = os.getenv("API_SECRET")
    signatureBuilder = (((((((((HmacSignatureBuilder()
                                .set_algorithm('sha512'))
                               .set_host("edw-bedrock-api.vpn.edw.dev.vonagenetworks.net"))
                              .set_method("POST"))
                             .set_resource("/edw-bedrock/invoke"))
                            .set_contentType("application/json"))
                           .set_date(date))
                          .set_nonce("XXXX"))
                         .set_apiKey(api_key))
                        .set_apiSecret(api_secret))


    sigByte = signatureBuilder.build()
    signature = signatureBuilder.buildAsBase64String()
    print(signature)
    return signature