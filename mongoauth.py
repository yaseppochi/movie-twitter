"""
Manage credentials for SSL connections to MongoDB on bigiron.
"""

from pymongo import MongoClient

class SSLMongoClient(MongoClient):
    def __init__(self, distinguished_name, host, user_cert, ca_certs, database):
        self.distinguished_name = distinguished_name
        self.host = host
        self.user_cert = user_cert
        self.ca_certs = ca_certs
        self.database_name = database
        self.databases = []

    def connect(self):
        self.client = MongoClient(host=self.host,
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
    database="admin"
    )

anna = SSLMongoClient(
    distinguished_name = "emailAddress=jdzoomer04@gmail.com,"
                         "CN=Anna Chen,OU=Graduate Seminar,"
                         "O=Turnbull Laboratory,ST=Ibaraki,C=JP",
    host="uwakimon.sk.tsukuba.ac.jp",
    user_cert="~/anna-cert.pem",      # implies ssl=true
    ca_certs="~/cacert.pem",
    database="admin"
    )

