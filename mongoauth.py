from pymongo import MongoClient as MC
db=MC(host="127.0.0.1",
    ssl_certfile="sensei-cert.pem",   # implies ssl=true
    ssl_ca_certs="demoCA/cacert.pem"
    ).admin
db.authenticate(name="emailAddress=turnbull@sk.tsukuba.ac.jp,CN=Sensei,OU=Admin,O=Turnbull Laboratory,ST=Ibaraki,C=JP", mechanism="MONGODB-X509")
