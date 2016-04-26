#!/usr/bin/python -u

import sys
import logging
import logging.config
import argparse
import pycurl
from StringIO import StringIO


class Pilot:
    """ Main class """

    def __init__(self):
        self.argParser = argparse.ArgumentParser(description="This is simplepilot. It will start your task... maybe..."
                                                             " in some distant future... on a specific environment..."
                                                             " with the help of some magic...")
        self.argParser.add_argument("--logconf", type=logging.config.fileConfig, default="loggers.ini",
                       help="specify logger parameters file", metavar="path/to/loggers.ini")
        self.argParser.add_argument("--cacert", type=logging.config.fileConfig, default="",
                       help="specify CA certificate for your transactions to server", metavar="path/to/x509_uXXX")

        self.logger = logging.getLogger("pilot")

    def run(self, argv):
        self.argParser.parse_args(sys.argv[1:])

        self.logger = logging.getLogger("pilot")
        self.logger.info("test")

    def create_curl(self, buf = 0):
        c = pycurl.Curl()
        if buf == 0:
            buf = StringIO()
        c.setopt(c.WRITEDATA, buf)
        c.setopt(c.CAPATH, )

    def get_queue(self):
        # curl --connect-timeout 20 --max-time 120 --cacert /tmp/x509up_u500 -sS \"http://pandaserver.cern.ch:25085/cache/schedconfig/ANALY_RRC-KI-HPC.all.json\" > /home/apf/dan_minipilot/queuedata.json
        self.logger.info("test")


# main
if __name__ == "__main__":
    pilot = Pilot()

    pilot.run(sys.argv)




