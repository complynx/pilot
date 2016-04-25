#!/usr/bin/python -u

import sys
import logging
import logging.config
import argparse

class Pilot:
    """ Main class """

    def __init__(self):
        self.argParser = argparse.ArgumentParser(description="This is simplepilot. It will start your task... maybe... in"
                                                       " some distant future... on a specific environment... with "
                                                       "the help of some magic...")
        self.argParser.add_argument("--logconf", type=logging.config.fileConfig, default="loggers.ini",
                       help="specify logger parameters file", metavar="path/to/loggers.ini")

        self.logger = logging.getLogger("pilot")

    def run(self, argv):
        self.argParser.parse_args(sys.argv[1:])

        self.logger = logging.getLogger("pilot")
        self.logger.info("test")

# main
if __name__ == "__main__":
    pilot = Pilot()

    pilot.run(sys.argv)




