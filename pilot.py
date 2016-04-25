#!/usr/bin/python -u

import sys
import logging
import logging.config
import argparse

# main
if __name__ == "__main__":

    argparser = argparse.ArgumentParser(description="This is simplepilot. It will start your task... maybe... in"
                                                           " some distant future... on a specific environment... with "
                                                           "the help of some magic...")
    argparser.add_argument("--logconf", type=logging.config.fileConfig, default="loggers.ini",
                           help="specify logger parameters file")

    # logging.config.fileConfig("loggers.ini")
    logger = logging.getLogger("pilot")
    logger.info("test")



