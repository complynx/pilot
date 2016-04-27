#!/usr/bin/python -u

import sys
import os
import logging
import logging.config
import argparse
import pycurl
from StringIO import StringIO
import json
import cpuinfo
import urllib
import urlparse
import psutil
import socket
import re

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
        self.argParser.add_argument("--jobserver", default="aipanda007.cern.ch",
                                    help="Panda job server web address.",
                                    metavar="pandajob.example.com")
        self.argParser.add_argument("--pandaserver_port", default=25085,
                                    type=int,
                                    help="Panda server port.",
                                    metavar="PORT")
        self.argParser.add_argument("--jobserver_port", default=25443,
                                    type=int,
                                    help="Panda job server port.",
                                    metavar="PORT")

        testqueuedata = "queuedata.json" if os.path.isfile("queuedata.json") else ""
        self.argParser.add_argument("--queuedata", default=testqueuedata,
                                    type=lambda x: x if os.path.isfile(x) else testqueuedata,
                                    help="Preset queuedata file.",
                                    metavar="path/to/queuedata.json")
        self.argParser.add_argument("--queue", default='',
                                    help="Queue name",
                                    metavar="QUEUE_NAME")
        self.argParser.add_argument("--job_tag", default='prod',
                                    help="Job type tag. Eg. test, user, prod, etc...",
                                    metavar="tag")
        self.argParser.add_argument("--job_description", default=None,
                                    type=lambda x: x if os.path.isfile(x) else None,
                                    help="Job description file, preloaded from server. The contents must be "
                                         "application/x-www-form-urlencoded string. Later may be JSON also.",
                                    metavar="tag")

        self.logger = logging.getLogger("pilot")
        self.sslCert = ""
        self.sslPath = ""
        self.sslCertOrPath = ""

    # @staticmethod
    def parse_answer(self, string):
        trimmed = string.strip()
        self.logger.debug(re.search("^([\w-]+(=[\w-]*)?(&[\w-]+(=[\w-]*)?)*)?$", trimmed))
        if re.match("^([\w-]+(=[\w-]*)?(&[\w-]+(=[\w-]*)?)*)?$", trimmed):  # is application/x-www-form-urlencoded
            return urlparse.parse_qs(trimmed, True)
        return json.loads(trimmed)

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
        self.get_job()

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
                    queuedata = self.parse_answer(f.read())
                except:
                    pass
        if queuedata is None:
            buf = StringIO()
            c = self.create_curl()
            c.setopt(c.URL, "http://%s:%d/cache/schedconfig/%s.all.json" % (self.args.pandaserver,
                                                                            self.args.pandaserver_port,
                                                                            self.args.queue))
            c.setopt(c.WRITEFUNCTION, buf.write)
            c.perform()
            c.close()
            queuedata = self.parse_answer(buf.getvalue())
            buf.close()

        self.logger.debug("queuedata found: "+json.dumps(queuedata, indent=4))

    def get_job(self):
        jobDesc = None
        if self.args.job_description is not None:
            with open(self.args.job_description) as f:
                try:
                    str = f.read()
                    self.logger.info("file contents: "+str)
                    jobDesc = self.parse_answer(str)
                except:
                    pass
        if jobDesc is None:

            cpuInfo = cpuinfo.get_cpu_info()
            memInfo = psutil.virtual_memory()
            nodeName = socket.gethostbyaddr(socket.gethostname())[0]
            diskSpace = float(psutil.disk_usage(".").total)/1024./1024.
            # diskSpace = min(diskSpace, 14336)  # I doubt this is necessary, so RM

            if "_CONDOR_SLOT" in os.environ:
                nodeName = os.environ.get("_CONDOR_SLOT", '')+"@"+nodeName

            data = {
                'cpu': float(cpuInfo['hz_actual_raw'][0])/1000000.,
                'mem': float(memInfo.total)/1024./1024.,
                'node': nodeName,
                'diskSpace': diskSpace,
                'getProxyKey': False,  # do we need it?
                'computingElement': self.args.queue,
                'siteName': self.args.queue,
                'workingGroup': '',  # do we need it?
                'prodSourceLabel': self.args.job_tag
            }

            buf = StringIO()
            c = self.create_curl()
            c.setopt(c.URL, "https://%s:%d/server/panda/getJob" % (self.args.jobserver,
                                                                   self.args.jobserver_port))
            c.setopt(c.WRITEFUNCTION, buf.write)
            c.setopt(c.POSTFIELDS, urllib.urlencode(data))
            # c.setopt(c.COMPRESS, True)
            c.setopt(c.SSL_VERIFYPEER, False)
            if self.sslCert != "":
                c.setopt(c.SSLCERT, self.sslCert)
                c.setopt(c.SSLKEY, self.sslCert)
            if self.sslPath != "":
                c.setopt(c.CAPATH, self.sslPath)
            c.setopt(c.SSL_VERIFYPEER, False)
            # c.setopt(c.USE_SSL, True)
            c.perform()
            c.close()
            jobDesc = self.parse_answer(buf.getvalue())
            buf.close()

        self.logger.debug("got job description: "+json.dumps(jobDesc))


# main
if __name__ == "__main__":
    pilot = Pilot()

    pilot.run(sys.argv)




