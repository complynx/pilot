#!/usr/bin/python -u

import sys
import logging
import logging.config
# import argparse

# main
if __name__ == "__main__":
    logging.config.fileConfig("loggers.ini")
    logging.info("test")

