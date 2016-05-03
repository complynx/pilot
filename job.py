import pilot
import urllib
import os
import commands
import json


class Job:
    __state = "sent"
    pilot = None
    description = None
    error_code = None
    id = None
    command = 'ls'
    input_files = []
    output_files = []
    log_file = ''
    no_update = False

    def __init__(self, _pilot, _desc):
        self.pilot = _pilot
        if _pilot.args.no_job_update:
            self.no_update = True
        self.description = _desc
        self.parse_description()

    def parse_description(self):
        self.id = self.description["PandaID"]
        self.command = self.description["transformation"]+" "+self.description["jobPars"]

    def convert_null(self, str):
        return str if str != "NULL" else None

    def extract_input_files(self):
        if self.description['inFiles'] and self.description['inFiles'] != "NULL":
            inFiles = self.description["inFiles"].split(',')
            ddmEndPointIn = self.description["ddmEndPointIn"].split(',')
            destinationSE = self.description["destinationSE"].split(',')
            dispatchDBlockToken = self.description["dispatchDBlockToken"].split(',')
            realDataset = self.description["realDataset"].split(',')
            fsize = self.description["fsize"].split(',')
            checksum = self.description["checksum"].split(',')

            for i, f in enumerate(inFiles):
                self.input_files[i] = {
                    "name": self.convert_null(f),
                    "ddm_endpoint": self.convert_null(ddmEndPointIn[i]),
                    "destinationSE": self.convert_null(destinationSE[i]),
                    "dispatchDBlockToken": self.convert_null(dispatchDBlockToken[i]),
                    "realDataset": self.convert_null(realDataset[i]),
                    "fsize": long(self.convert_null(fsize[i])),
                    "checksum": self.convert_null(checksum[i])
                }

        self.pilot.logger.debug("extracted files: "+json.dumps(self.input_files, indent=4))

    @property
    def state(self):
        return self.__state

    def send_state(self):
        """
        Sends job state to the dedicated panda server.
        """
        if self.no_update:
            self.pilot.logger.info("Configured without server updates, skip.")
        else:
            self.pilot.logger.info("Updating server job status...")
            data = {
                'node': self.pilot.node_name,
                'state': self.state,
                'jobId': self.id,
                # 'pilotID': self.pilot_id,
                'timestamp': self.pilot.time_iso8601(),
                'workdir': os.getcwd()
            }

            if self.error_code is not None:
                data["exeErrorCode"] = self.error_code

            _str = self.pilot.curl_query("https://%s:%d/server/panda/updateJob" % (self.pilot.args.jobserver,
                                                                                   self.pilot.args.jobserver_port),
                                         ssl=True, body=urllib.urlencode(data))
            self.pilot.logger.debug("Got from server: " + _str)
            # jobDesc = json.loads(_str)
            # self.logger.info("Got from server: " % json.dumps(jobDesc, indent=4))

    @state.setter
    def state(self, value):
        """
        Sets new state and updates server.

        :param value: new job state.
        """
        if value != self.__state:
            self.pilot.logger.info("Setting job state of job %s to %s" % (self.id, self.state))
            self.__state = value
            self.send_state()

    def run(self):
        self.state('starting')
        self.state('stagein')

        self.state = 'running'

        self.pilot.logger.info("Starting job cmd: %s" % self.command)
        s, o = commands.getstatusoutput(self.command)

        self.pilot.logger.info("Job ended with status: %d" % s)
        self.pilot.logger.info("Job output:\n%s" % o)
        self.error_code = s

        self.state = "holding"
        self.state = 'finished'
