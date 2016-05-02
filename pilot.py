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
import psutil
import socket
import platform
import pip
import time
import commands
import traceback


class Pilot:
    """
    Main class.

    This class holds all the stuff and does all the things.
    """

    user_agent = 'Pilot/2.0'
    sslCert = ""
    sslPath = ""
    sslCertOrPath = ""
    args = None
    executable = __file__
    queuedata = None

    def __init__(self):
        """
        Initialization. Mostly setting up argparse, but also a few lines of resolving some early variables.
        :return:
        """
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

        testqueuedata = "queuedata.json" if os.path.isfile("queuedata.json") else None
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
        self.user_agent += " (Python %s; %s %s; rv:alpha) minipilot/daniel" % \
                           (sys.version.split(" ")[0],
                            platform.system(), platform.machine())

        self.node_name = socket.gethostbyaddr(socket.gethostname())[0]
        if "_CONDOR_SLOT" in os.environ:
            self.node_name = os.environ.get("_CONDOR_SLOT", '')+"@"+self.node_name

        self.pilot_id = self.node_name+(":%d" % os.getpid())

    def init_after_arguments(self):
        """
        Second step of initialization. After arguments received, pilot needs to set up some other variables.
        :return:
        """
        if os.path.exists(self.args.cacert):
            self.sslCert = self.args.cacert
        if os.path.exists(self.args.capath):
            self.sslPath = self.args.capath

        self.sslCertOrPath = self.sslCert if self.sslCert != "" else self.sslPath

        self.logger = logging.getLogger("pilot")

    def print_initial_information(self):
        """
        Pilot is initialized somehow, this initialization needs to be print out for information.
        :return:
        """
        if self.args is not None:
            self.logger.info("Pilot is running.")
            self.logger.info("Started with arguments %s" % vars(self.args))
        self.logger.info("User-Agent: " + self.user_agent)

        self.logger.info("Pilot is started from %s" % self.dir)
        self.logger.info("Working directory is %s" % os.getcwd())

        self.logger.info("Printing requirements versions...")
        requirements = pip.req.parse_requirements(os.path.join(self.dir,
                                                               "requirements.txt"),
                                                  session=False)
        for req in requirements:
            self.logger.info("%s (%s)" % (req.name, req.installed_version))

    def run(self, argv):
        """
        Main execution entrance point.
        :param argv: command line arguments
        :return:
        """
        self.executable = argv[0]
        self.args = self.argParser.parse_args(argv[1:])
        self.init_after_arguments()

        self.print_initial_information()

        try:
            self.get_queuedata()
            job_desc = self.get_job()
            self.run_job(job_desc)
        except:
            self.logger.error("During the run encountered uncaught exception.")
            self.logger.error(traceback.format_exc())
            pass

    @staticmethod
    def time_iso8601(t=time.localtime(), timezone=time.timezone):
        """
        :param time(t): time to format down. Default to now.
        :param timezone: timezone of requested time. Default to local timezone.
        :return str: ISO-8601 compliant date/time string, timezone included
        """

        if timezone > 0:
            tz_sign = '-'
        else:
            tz_sign = '+'
        timezone_hours = int(timezone/3600)

        return str("%s%s%02d%02d" % (time.strftime("%Y-%m-%dT%H:%M:%S", t), tz_sign, timezone_hours,
                                     int(timezone/60-timezone_hours*60)))

    def curl_query(self, url, body=None, **kwargs):
        """
        Send query to server using cURL library. For simpleness does not test anything.

        :param url: URL of the resource
        :param body: string to be sent, if any.
        ... params to be passed to create_curl

        :return str: server response.
        """
        buf = StringIO()
        c = self.create_curl(**kwargs)
        c.setopt(c.URL, url)
        c.setopt(c.WRITEFUNCTION, buf.write)
        if body is not None:
            c.setopt(c.POSTFIELDS, body)
        c.perform()
        c.close()
        _str = str(buf.getvalue())
        buf.close()
        return _str

    def send_job_state(self, job_desc, state):
        """
        Sends job state to the dedicated panda server.

        :param job_desc: job description.
        :param state: new job state.
        """
        self.logger.info("Setting job state of job %s to %s" % (job_desc["PandaID"], state))
        data = {
            'node': self.node_name,
            'state': state,
            'jobId': job_desc["PandaID"],
            # 'pilotID': self.pilot_id,
            'timestamp': self.time_iso8601(),
            'workdir': os.getcwd()
        }

        if job_desc["exeErrorCode"] is not None:
            data["exeErrorCode"] = job_desc["exeErrorCode"]

        _str = self.curl_query("https://%s:%d/server/panda/updateJob" % (self.args.jobserver,
                                                                         self.args.jobserver_port),
                               ssl=True, body=urllib.urlencode(data))
        self.logger.debug("Got from server: "+_str)
        # jobDesc = json.loads(_str)
        # self.logger.info("Got from server: " % json.dumps(jobDesc, indent=4))

    def run_job(self, job_desc):
        """
        Runs actual received job. Currently without stage-in/stage-out, but there will be they.

        :param job_desc: job description returned by the server.
        """
        job_desc["exeErrorCode"] = None
        self.send_job_state(job_desc, "starting")

        self.send_job_state(job_desc, "running")

        cmd = job_desc["transformation"]+" "+job_desc["jobPars"]

        self.logger.info("Starting job cmd: %s" % cmd)
        s, o = commands.getstatusoutput(cmd)

        self.logger.info("Job ended with status: %d" % s)
        self.logger.info("Job output:\n%s" % o)
        job_desc["exeErrorCode"] = s

        self.send_job_state(job_desc, "holding")
        self.send_job_state(job_desc, "finished")

    def create_curl(self, ssl=False):
        """
        Creates cURL interface instance with required options and headers.
        :param Boolean(ssl): whether to set up SSL params or not. Default False.

        :return pycurl.Curl: cURL interface class
        """
        c = pycurl.Curl()
        if self.sslCertOrPath != "":
            c.setopt(c.CAPATH, self.sslCertOrPath)
        c.setopt(c.CONNECTTIMEOUT, 20)
        c.setopt(c.TIMEOUT, 120)
        c.setopt(c.HTTPHEADER, ['Accept: application/json;q=0.9,'
                                'text/html,application/xhtml+xml,application/xml;q=0.7,*/*;q=0.5',
                                'User-Agent: ' + self.user_agent])
        if ssl:
            if self.sslCert != "":
                c.setopt(c.SSLCERT, self.sslCert)
                c.setopt(c.SSLKEY, self.sslCert)
            if self.sslPath != "":
                c.setopt(c.CAPATH, self.sslPath)
            c.setopt(c.SSL_VERIFYPEER, False)
        return c

    def try_get_json_file(self, file_name):
        """
        Tries to read a file and parse it as JSON. All exceptions converted to warnings.

        :param file_name:

        :return: parsed JSON object or None on failure.
        """
        if isinstance(file_name, basestring) and file_name != "" and os.path.isfile(file_name):
            self.logger.info("Trying to fetch JSON local file %s." % file_name)
            try:
                with open(file_name) as f:
                    j = json.load(f)
                    self.logger.info("Successfully loaded file and parsed.")
                    return j
            except Exception as e:
                self.logger.warning(str(e))
                self.logger.warning("File loading and parsing failed.")
                pass
        return None

    def get_queuedata(self):
        """
        Retrieve queuedata from file or from server and store it into Pilot.queuedata.
        """
        self.logger.info("Trying to get queuedata.")
        self.queuedata = self.try_get_json_file(self.args.queuedata)
        if self.queuedata is None:
            self.logger.info("Queuedata is not saved locally. Asking server.")

            _str = self.curl_query("http://%s:%d/cache/schedconfig/%s.all.json" % (self.args.pandaserver,
                                                                                   self.args.pandaserver_port,
                                                                                   self.args.queue))

            self.queuedata = json.loads(_str)

        self.logger.info("Queuedata obtained.")
        # self.logger.debug("queuedata: "+json.dumps(queuedata, indent=4))

    def get_job(self):
        """
        Gets job description from a file or from server.

        :return: job description.
        """
        self.logger.info("Trying to get job description.")
        job_desc = self.try_get_json_file(self.args.job_description)
        if job_desc is None:
            self.logger.info("Job description is not saved locally. Asking server.")
            cpu_info = cpuinfo.get_cpu_info()
            mem_info = psutil.virtual_memory()
            disk_space = float(psutil.disk_usage(".").total)/1024./1024.
            # diskSpace = min(diskSpace, 14336)  # I doubt this is necessary, so RM

            data = {
                'cpu': float(cpu_info['hz_actual_raw'][0])/1000000.,
                'mem': float(mem_info.total)/1024./1024.,
                'node': self.node_name,
                'diskSpace': disk_space,
                'getProxyKey': False,  # do we need it?
                'computingElement': self.args.queue,
                'siteName': self.args.queue,
                'workingGroup': '',  # do we need it?
                'prodSourceLabel': self.args.job_tag
            }

            _str = self.curl_query("https://%s:%d/server/panda/updateJob" % (self.args.jobserver,
                                                                             self.args.jobserver_port),
                                   ssl=True, body=urllib.urlencode(data))
            # self.logger.debug("Got from server: "+_str)
            try:
                job_desc = json.loads(_str)
            except ValueError:
                self.logger.error("JSON parser failed.")
                self.logger.error("Got from server: "+_str)
                raise

        self.logger.info("Got job description.")
        self.logger.debug("Job description: "+json.dumps(job_desc, indent=4))
        return job_desc


# main
if __name__ == "__main__":
    """
    Main workflow is to create Pilot instance and run it with command arguments.
    """
    pilot = Pilot()
    pilot.run(sys.argv)




