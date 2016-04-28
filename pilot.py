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
import platform
import pip
import time
import commands


class Pilot:
    """ Main class """

    user_agent = 'Pilot/2.0'

    def __init__(self):
        self.dir = os.path.dirname(os.path.realpath(__file__))

        self.argParser = argparse.ArgumentParser(description="This is simplepilot. It will start your task... maybe..."
                                                             " in some distant future... on a specific environment..."
                                                             " with the help of some magic...")
        self.argParser.add_argument("--logconf", type=logging.config.fileConfig, default=os.path.join(self.dir,
                                                                                                      "loggers.ini"),
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
        self.args = None
        self.user_agent += " (Python %s; %s %s; rv:alpha) minipilot/daniel" % \
                           (sys.version.split(" ")[0],
                            platform.system(), platform.machine())

        self.node_name = socket.gethostbyaddr(socket.gethostname())[0]
        if "_CONDOR_SLOT" in os.environ:
            self.node_name = os.environ.get("_CONDOR_SLOT", '')+"@"+self.node_name

        self.pilot_id = self.node_name+(":%d" % os.getpid())

    def test_certificate_info(self):
        if os.path.exists(self.args.cacert):
            self.sslCert = self.args.cacert
        if os.path.exists(self.args.capath):
            self.sslPath = self.args.capath

        self.sslCertOrPath = self.sslCert if self.sslCert != "" else self.sslPath

    def print_initial_information(self):
        if self.args is not None:
            self.logger.info("Pilot is running.")
            self.logger.info("Started with arguments %s" % vars(self.args))
        self.logger.info("User-Agent: " + self.user_agent)

        self.logger.info("Pilot is started from %s" % self.dir)
        self.logger.info("Working directory is %s" % os.getcwd())

        self.logger.info("Testing requirements...")
        requirements = pip.req.parse_requirements(os.path.join(self.dir,
                                                               "requirements.txt"),
                                                  session=False)
        for req in requirements:
            self.logger.info("%s (%s)" % (req.name, req.installed_version))

    def run(self, argv):
        self.args = self.argParser.parse_args(sys.argv[1:])
        self.test_certificate_info()

        self.logger = logging.getLogger("pilot")
        self.print_initial_information()

        self.get_queuedata()
        job_desc = self.get_job()
        self.run_job(job_desc)

    @staticmethod
    def time_stamp(t=time.localtime()):
        """ return ISO-8601 compliant date/time format """

        tmptz = time.timezone
        if tmptz > 0:
            signstr = '-'
        else:
            signstr = '+'
        tmptz_hours = int(tmptz/3600)

        return str("%s%s%02d%02d" % (time.strftime("%Y-%m-%dT%H:%M:%S", t), signstr, tmptz_hours,
                                     int(tmptz/60-tmptz_hours*60)))

    def send_job_state(self, job_desc, state):
        self.logger.info("Setting job state of job %s to %s" % (job_desc["PandaID"], state))
        data = {
            'node': self.node_name,
            'state': state,
            'jobId': job_desc["PandaID"],
            'pilotID': self.pilot_id,
            'timestamp': self.time_stamp(),
            'workdir': os.getcwd()
        }

        buf = StringIO()
        c = self.create_curl()
        c.setopt(c.URL, "https://%s:%d/server/panda/updateJob" % (self.args.jobserver,
                                                                  self.args.jobserver_port))
        c.setopt(c.WRITEFUNCTION, buf.write)
        c.setopt(c.POSTFIELDS, urllib.urlencode(data))
        c.setopt(c.SSL_VERIFYPEER, False)
        if self.sslCert != "":
            c.setopt(c.SSLCERT, self.sslCert)
            c.setopt(c.SSLKEY, self.sslCert)
        if self.sslPath != "":
            c.setopt(c.CAPATH, self.sslPath)
        c.setopt(c.SSL_VERIFYPEER, False)
        c.perform()
        c.close()
        jobDesc = json.loads(buf.getvalue())
        buf.close()
        self.logger.info("Got from server: " % json.dumps(jobDesc, indent=4))

    def run_job(self, job_desc):
        self.send_job_state(job_desc, "starting")
        self.send_job_state(job_desc, "running")
        cmd = job_desc["trfName"]+" "+job_desc["jobPars"]
        self.logger.info("Starting job cmd: %s" % cmd)
        s, o = commands.getstatusoutput(cmd)
        self.logger.info("Job ended with status: %d" % s)
        self.logger.info("Job output:\n%s" % o)
        self.send_job_state(job_desc, "holding")

    def create_curl(self):
        c = pycurl.Curl()
        if self.sslCertOrPath != "":
            c.setopt(c.CAPATH, self.sslCertOrPath)
        c.setopt(c.CONNECTTIMEOUT, 20)
        c.setopt(c.TIMEOUT, 120)
        c.setopt(c.HTTPHEADER, ['Accept: application/json;q=0.9,'
                                'text/html,application/xhtml+xml,application/xml;q=0.7,*/*;q=0.5',
                                'User-Agent: ' + self.user_agent])
        return c

    def get_queuedata(self):
        queuedata = None
        if self.args.queuedata != "":
            self.logger.info("Trying to fetch queuedata from local file %s." % self.args.queuedata)
            with open(self.args.queuedata) as f:
                try:
                    queuedata = json.load(f)
                    self.logger.info("Successfully loaded file and parsed.")
                except:
                    self.logger.warning("File loading and parsing failed.")
                    pass
        if queuedata is None:
            self.logger.info("Queuedata is not saved locally. Asking server.")

            buf = StringIO()
            c = self.create_curl()
            c.setopt(c.URL, "http://%s:%d/cache/schedconfig/%s.all.json" % (self.args.pandaserver,
                                                                            self.args.pandaserver_port,
                                                                            self.args.queue))
            c.setopt(c.WRITEFUNCTION, buf.write)
            c.perform()
            c.close()
            queuedata = json.loads(buf.getvalue())
            buf.close()

        self.logger.info("Queuedata obtained.")
        # self.logger.debug("queuedata: "+json.dumps(queuedata, indent=4))

    def get_job(self):
        jobDesc = None
        if self.args.job_description is not None:
            self.logger.info("Trying to fetch job description from local file %s." % self.args.job_description)
            with open(self.args.job_description) as f:
                try:
                    jobDesc = json.load(f)
                    self.logger.info("Successfully loaded file and parsed.")
                except:
                    self.logger.warnig("File loading and parsing failed.")
                    pass
        if jobDesc is None:
            cpuInfo = cpuinfo.get_cpu_info()
            memInfo = psutil.virtual_memory()
            diskSpace = float(psutil.disk_usage(".").total)/1024./1024.
            # diskSpace = min(diskSpace, 14336)  # I doubt this is necessary, so RM

            data = {
                'cpu': float(cpuInfo['hz_actual_raw'][0])/1000000.,
                'mem': float(memInfo.total)/1024./1024.,
                'node': self.node_name,
                'diskSpace': diskSpace,
                'pilotID': self.pilot_id,
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
            c.setopt(c.SSL_VERIFYPEER, False)
            if self.sslCert != "":
                c.setopt(c.SSLCERT, self.sslCert)
                c.setopt(c.SSLKEY, self.sslCert)
            if self.sslPath != "":
                c.setopt(c.CAPATH, self.sslPath)
            c.setopt(c.SSL_VERIFYPEER, False)
            c.perform()
            c.close()
            jobDesc = json.loads(buf.getvalue())
            buf.close()

        self.logger.info("Got job description.")
        # self.logger.debug("Job description: "+json.dumps(jobDesc, indent=4)
        return jobDesc


# main
if __name__ == "__main__":
    pilot = Pilot()

    pilot.run(sys.argv)




