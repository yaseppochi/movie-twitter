"""
Manage credentials for SSL connections to MongoDB on bigiron.
"""

from os.path import expanduser
from pymongo import MongoClient

class SSLMongoClient(MongoClient):
    def __init__(self, distinguished_name, host, user_cert, ca_certs, database,
                 port=27017):
        self.distinguished_name = distinguished_name
        self.host = host
        self.port = port
        self.user_cert = expanduser(user_cert)
        self.ca_certs = expanduser(ca_certs)
        self.database_name = database
        self.databases = []

    def connect(self):
        self.client = MongoClient(host=self.host,
                                  port=self.port,
                                  ssl_certfile=self.user_cert,
                                  ssl_ca_certs=self.ca_certs)
        db = self.client[self.database_name]
        db.authenticate(name=self.distinguished_name,
                        mechanism="MONGODB-X509")
        self.databases.append(db)
        return db

local = SSLMongoClient(
    distinguished_name = "emailAddress=turnbull@sk.tsukuba.ac.jp,"
                         "CN=Sensei,OU=Admin,"
                         "O=Turnbull Laboratory,ST=Ibaraki,C=JP",
    host="127.0.0.1",
    user_cert="sensei-cert.pem",   # implies ssl=true
    ca_certs="demoCA/cacert.pem",
    database="admin"
    )

sensei = SSLMongoClient(
    distinguished_name = "emailAddress=turnbull@sk.tsukuba.ac.jp,"
                         "CN=Sensei,OU=Admin,"
                         "O=Turnbull Laboratory,ST=Ibaraki,C=JP",
    host="uwakimon.sk.tsukuba.ac.jp",
    user_cert="~/sensei-cert.pem",      # implies ssl=true
    ca_certs="~/cacert.pem",
    database="admin"
    )

steve = SSLMongoClient(
    distinguished_name = "emailAddress=turnbull@sk.tsukuba.ac.jp,"
                         "CN=Stephen Turnbull,OU=Graduate Seminar,"
                         "O=Turnbull Laboratory,ST=Ibaraki,C=JP",
    host="uwakimon.sk.tsukuba.ac.jp",
    user_cert="~/steve-cert.pem",       # implies ssl=true
    ca_certs="~/cacert.pem",
    database="anna"
    )

tunnel = SSLMongoClient(
    distinguished_name = "emailAddress=turnbull@sk.tsukuba.ac.jp,"
                         "CN=Stephen Turnbull,OU=Graduate Seminar,"
                         "O=Turnbull Laboratory,ST=Ibaraki,C=JP",
    host="localhost",
    port=28108,
    user_cert="~/steve-cert.pem",       # implies ssl=true
    ca_certs="~/cacert.pem",
    database="anna"
    )

anna = SSLMongoClient(
    distinguished_name = "emailAddress=jdzoomer04@gmail.com,"
                         "CN=Anna Chen,OU=Graduate Seminar,"
                         "O=Turnbull Laboratory,ST=Ibaraki,C=JP",
    host="uwakimon.sk.tsukuba.ac.jp",
    user_cert="~/anna-cert.pem",      # implies ssl=true
    ca_certs="~/cacert.pem",
    database="anna"
    )

# Build IndexModels.
# More for documentation of existing indices than use.
from pymongo import IndexModel, ASCENDING, TEXT
id_IM = IndexModel([("id", ASCENDING)],
                   sparse=True)
lang_IM = IndexModel([("lang", ASCENDING)],
                     sparse=True)
timestamp_IM = IndexModel([("timestamp_ms", ASCENDING)],
                          sparse=True)
serial_number_IM = IndexModel([("anna:serial", ASCENDING)],
                              sparse=True)
twitter_text_IM = IndexModel([("text", TEXT),
                              ("entities.urls.expanded_url", TEXT),
                              ("entities.urls.display_url", TEXT),
                              ("entities.media.expanded_url", TEXT),
                              ("entities.media.display_url", TEXT),
                              ("entities.hashtags.text", TEXT),
                              ("entities.user_mentions.screen_name", TEXT)],
                             default_language="english",
                             name="twitter_text")
