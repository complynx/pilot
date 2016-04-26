#!/usr/bin/python -u

import sys
import os
import logging
import logging.config
import argparse
import pycurl
from StringIO import StringIO
import json

class Pilot:
    """ Main class """

    def __init__(self):
        self.argParser = argparse.ArgumentParser(description="This is simplepilot. It will start your task... maybe..."
                                                             " in some distant future... on a specific environment..."
                                                             " with the help of some magic...")
        self.argParser.add_argument("--logconf", type=logging.config.fileConfig, default="loggers.ini",
                                    help="specify logger parameters file", metavar="path/to/loggers.ini")
        self.argParser.add_argument("--cacert", default=os.environ.get('X509_USER_PROXY',
                                                                       '/tmp/x509up_u%s' % str(os.getuid())),
                                    help="specify CA certificate or path for your transactions to server",
                                    metavar="path/to/your/certificate")
        self.argParser.add_argument("--capath", default=os.environ.get('X509_CERT_DIR',
                                                                       '/etc/grid-security/certificates'),
                                    help="specify CA path for certificates",
                                    metavar="path/to/certificates/")
        self.argParser.add_argument("--pandaserver", default="pandaserver.cern.ch",
                                    help="Panda server web address.",
                                    metavar="panda.example.com")
        self.argParser.add_argument("--pandaserver_port", default=25085,
                                    type=int,
                                    help="Panda server port.",
                                    metavar="PORT")

        testqueuedata = "queuedata.json" if os.path.isfile("queuedata.json") else ""
        self.argParser.add_argument("--queuedata", default=testqueuedata,
                                    type=lambda x: x if os.path.isfile(x) else testqueuedata,
                                    help="Preset queuedata file.",
                                    metavar="path/to/queuedata.json")
        self.argParser.add_argument("--queue", default='',
                                    help="Queue name",
                                    metavar="QUEUE_NAME")

        self.logger = logging.getLogger("pilot")
        self.sslCert = ""
        self.sslPath = ""
        self.sslCertOrPath = ""

    def test_certificate_info(self):
        if os.path.exists(self.args.cacert):
            self.sslCert = self.args.cacert
        if os.path.exists(self.args.capath):
            self.sslPath = self.args.capath

        self.sslCertOrPath = self.sslCert if self.sslCert != "" else self.sslPath

    def run(self, argv):
        self.args = self.argParser.parse_args(sys.argv[1:])
        self.test_certificate_info()

        self.logger = logging.getLogger("pilot")
        self.logger.info("Pilot running")
        self.get_queuedata()

    def create_curl(self):
        c = pycurl.Curl()
        if self.sslCertOrPath != "":
            c.setopt(c.CAPATH, self.sslCertOrPath)
        c.setopt(c.CONNECTTIMEOUT, 20)
        c.setopt(c.TIMEOUT, 120)
        return c

    def get_queuedata(self):
        queuedata = None
        if self.args.queuedata != "":
            with open(self.args.queuedata) as f:
                try:
                    queuedata = json.load(f)
                except ValueError:
                    pass
        if queuedata is None:
            buf = StringIO()
            c = self.create_curl()
            c.setopt(c.URL, "http://%s:%d/cache/schedconfig/%s.all.json" % (self.args.pandaserver,
                                                                            self.args.pandaserver_port,
                                                                            self.args.queue))
            c.setopt(c.WRITEDATA, buf)
            c.perform()
            c.close()
            queuedata = json.load(buf)
            buf.close()

        # curl --connect-timeout 20 --max-time 120 --cacert /tmp/x509up_u500 -sS \"http://pandaserver.cern.ch:25085/cache/schedconfig/ANALY_RRC-KI-HPC.all.json\" > /home/apf/dan_minipilot/queuedata.json
        self.logger.info("queuedata found: "+json.dumps(queuedata, ident=4))


# main
if __name__ == "__main__":
    pilot = Pilot()

    pilot.run(sys.argv)




