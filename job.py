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
    input_files = {}
    output_files = {}
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

        self.extract_input_files()
        self.extract_output_files()
        self.extract_log_file()

    def convert_null(self, val):
        return val if val != "NULL" else None

    def extract_input_files(self):
        if self.description['inFiles'] and self.description['inFiles'] != "NULL":
            in_files = self.description["inFiles"].split(',')
            ddmEndPointIn = self.description["ddmEndPointIn"].split(',')
            destinationSE = self.description["destinationSE"].split(',')
            dispatchDBlockToken = self.description["dispatchDBlockToken"].split(',')
            realDatasetsIn = self.description["realDataseprodDBlockstsIn"].split(',')
            prodDBlocks = self.description["prodDBlocks"].split(',')
            fsize = self.description["fsize"].split(',')
            c_sum = self.description["checksum"].split(',')

            for i, f in enumerate(in_files):
                if f != "NULL":
                    self.input_files[f] = {
                        "ddm_endpoint": self.convert_null(ddmEndPointIn[i]),
                        "destinationSE": self.convert_null(destinationSE[i]),
                        "dispatchDBlockToken": self.convert_null(dispatchDBlockToken[i]),
                        "realDataset": self.convert_null(realDatasetsIn[i]),
                        "prodDBlock": self.convert_null(prodDBlocks[i]),
                        "fsize": long(self.convert_null(fsize[i])),
                        "checksum": self.convert_null(c_sum[i])
                    }

        self.pilot.logger.debug("extracted files: "+json.dumps(self.input_files, indent=4))

    def extract_output_files(self):
        if self.description['outFiles'] and self.description['outFiles'] != "NULL":
            files = self.description["outFiles"].split(',')
            ddmEndPointOut = self.description["ddmEndPointOut"].split(',')
            fileDestinationSE = self.description["fileDestinationSE"].split(',')
            dispatchDBlockTokenForOut = self.description["dispatchDBlockTokenForOut"].split(',')
            destinationDBlockToken = self.description["destinationDBlockToken"].split(',')
            realDatasets = self.description["realDatasets"].split(',')
            destinationDblock = self.description["destinationDblock"].split(',')

            for i, f in enumerate(files):
                if f != "NULL":
                    self.output_files[f] = {
                        "ddm_endpoint": self.convert_null(ddmEndPointOut[i]),
                        "destinationSE": self.convert_null(fileDestinationSE[i]),
                        "dispatchDBlockToken": self.convert_null(dispatchDBlockTokenForOut[i]),
                        "destinationDBlockToken": self.convert_null(destinationDBlockToken[i]),
                        "realDataset": self.convert_null(realDatasets[i]),
                        "destinationDblock": self.convert_null(destinationDblock[i])
                    }

        self.pilot.logger.debug("extracted files: "+json.dumps(self.output_files, indent=4))

    def extract_log_file(self):
        if self.description["logFile"] and self.description["logFile"] != "NULL":
            self.log_file = self.description["logFile"]

            if self.description["logGUID"] and self.description["logGUID"] != "NULL" and self.log_file in\
                    self.output_files:
                self.output_files[self.log_file]['GUID'] = self.description["logGUID"]

        self.pilot.logger.debug("log file: "+self.log_file+" "+json.dumps(self.output_files[self.log_file], indent=4))

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
        self.state = 'starting'
        self.state = 'stagein'

        self.state = 'running'

        self.pilot.logger.info("Starting job cmd: %s" % self.command)
        s, o = commands.getstatusoutput(self.command)

        self.pilot.logger.info("Job ended with status: %d" % s)
        self.pilot.logger.info("Job output:\n%s" % o)
        self.error_code = s

        self.state = "holding"
        self.state = 'finished'
