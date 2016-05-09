import urllib
import os
import subprocess
import json
import shlex
import pipes
import re
import logging


class Job(object):
    __state = "sent"
    pilot = None
    description = None
    error_code = None
    no_update = False
    description_aliases = {
        'id': 'job_id'
    }
    acceptable_log_wrappers = ["tar", "tgz", "gz", "gzip", "tbz2", "bz2", "bzip2"]
    log_file = 'stub.job.log'
    log_wrapper = '.tgz'
    log = logging.getLogger()

    def __init__(self, _pilot, _desc):
        self.log = logging.getLogger('pilot.jobmanager')
        self.pilot = _pilot
        if _pilot.args.no_job_update:
            self.no_update = True
        self.description = _desc
        self.log.debug(json.dumps(self.description, indent=4, sort_keys=True))
        self.parse_description()

    def __getattr__(self, item):
        """
        Propagation of description values to Job instance if they are not shadowed.
        """
        try:
            return object.__getattribute__(self, item)
        except AttributeError:
            if self.description is not None:
                if item in self.description_aliases:
                    return self.description[self.description_aliases[item]]
                if item in self.description:
                    return self.description[item]
            raise

    def __setattr__(self, key, value):
        """
        Propagation of description values to Job instance if they are not shadowed.
        """
        try:
            old = object.__getattribute__(self, key)
            object.__setattr__(self, key, value)
        except AttributeError:
            if self.description is not None:
                if key in self.description_aliases:
                    self.description[self.description_aliases[key]] = value
                elif self.description is not None and key in self.description:
                    self.description[key] = value
                return
            object.__setattr__(self, key, value)

    def extract_queuedata_updates(self, job_parameters):
        """
        Extract the queuedata overwrite key=value pairs from the job parameters.
        Port from previous version, because this thing is not posix-compliant.
        Also it works awfully unpredictable, so I don't lke the whole of it.
        """
        # The dictionary will be used to overwrite existing queuedata values
        # --overwriteQueuedata={key1=value1,key2=value2}

        queuedata_update_dict = {}

        # define regexp pattern for the full overwrite command
        pattern = re.compile(r' \-\-overwriteQueuedata=\{.+}')
        full_update_string = re.findall(pattern, job_parameters)

        if full_update_string and full_update_string[0] != "":
            # tolog("Extracted the full command from the job parameters: %s" % (full_update_string[0]))
            # e.g. full_update_string[0] = '--overwriteQueuedata={key1=value1 key2=value2}'

            # remove the overwriteQueuedata command from the job parameters
            job_parameters = job_parameters.replace(full_update_string[0], "")
            self.log.info("Removed the queuedata overwrite command from job parameters: %s" % job_parameters)

            # define regexp pattern for the full overwrite command
            pattern = re.compile(r'\-\-overwriteQueuedata=\{(.+)\}')

            # extract the key value pairs string from the already extracted full command
            pairs = re.findall(pattern, full_update_string[0])
            # e.g. pairs[0] = 'key1=value1,key2=value2'

            if pairs[0] != "":
                # tolog("Extracted the key value pairs from the full command: %s" % (pairs[0]))

                # remove any extra spaces if present
                pairs[0] = pairs[0].replace(" ", "")

                comma_dictionary = {}
                if "\'" in pairs[0] or '\"' in pairs[0]:
                    log.info("Detected quotation marks in the job parameters: %s" % (pairs[0]))
                    # e.g. key1=value1,key2=value2,key3='value3,value4'

                    # handle quoted key-values separately

                    # replace any simple qoutation marks with double quotation marks to simplify the regexp below
                    pairs[0] = pairs[0].replace("\'", '\"')
                    pairs[0] = pairs[0].replace('\\"', '\"')  # in case double backslashes are present

                    # extract all values containing commas
                    comma_list = re.findall('"([^"]*)"', pairs[0])

                    # create a dictionary with key-values using format "key_%d" = value, where %d is the id of the found
                    #  value
                    # e.g. { key_1: valueX,valueY,valueZ, key_2: valueA,valueB }
                    # replace the original comma-containing value with "key_%d", and replace it later
                    comma_dictionary = {}
                    counter = 0
                    for commaValue in comma_list:
                        counter += 1
                        key = 'key_%d' % counter
                        comma_dictionary[key] = commaValue
                        pairs[0] = pairs[0].replace('\"' + commaValue + '\"', key)

                    self.log.info("pairs=%s" % (pairs[0]))
                    self.log.info("comma_dictionary=%s" % str(comma_dictionary))

                # define the regexp pattern for the actual key=value pairs
                # full backslash escape, see (adjusted for python):
                # http://stackoverflow.com/questions/168171/regular-expression-for-parsing-name-value-pairs
                pattern = re.compile(r'((?:\\.|[^=,]+)*)=("(?:\\.|[^"\\]+)*"|(?:\\.|[^,"\\]+)*)')

                # finally extract the key=value parameters
                kv_ist = re.findall(pattern, pairs[0])
                # e.g. kv_ist = [('key1', 'value1'), ('key2', 'value_2')]

                # put the extracted pairs in a proper dictionary
                if kv_ist:
                    self.log.info("Extracted the following key value pairs from job parameters: %s" %
                                           str(kv_ist))

                    for key, value in kv_ist:

                        if key != "":
                            # extract the value from the comma_dictionary if it exists
                            if value in comma_dictionary:
                                value = comma_dictionary[value]

                            queuedata_update_dict[key] = value
                        else:
                            self.log.warning("Bad key detected in key value tuple: %s" % str((key, value)))
                else:
                    self.log.warning("!!WARNING!!1223!! Failed to extract the key value pair list from: %s"
                                              % (pairs[0]))
            else:
                self.log.warning("!!WARNING!!1223!! Failed to extract the key value pairs from: %s" %
                                          (pairs[0]))
        else:
            self.log.warning("!!WARNING!!1223!! Failed to extract the full queuedata overwrite command from "
                                      "jobParameters=%s" % job_parameters)

        return job_parameters, queuedata_update_dict

    def modify_queuedata(self):
        params = self.command_parameters
        modifier = {}
        others = ""
        if isinstance(params, basestring) and '--overwriteQueuedata=' in params:
            others, modifier = self.extract_queuedata_updates(str(params))

            self.log.debug("overwrite: %s" % (modifier))
            self.log.debug("params: %s" % (others))

            for key in modifier:
                self.pilot.queuedata[key] = modifier[key]

            self.log.info("queuedata modified.")
            self.log.debug("queuedata: " + json.dumps(self.pilot.queuedata, indent=4))

    def init_logging(self):
        log_basename = self.description["log_file"]

        log_file = ''
        log_wrapper = ''

        for ext in self.acceptable_log_wrappers:
            if log_file != '':
                break
            log_file, dot_ext, rest = log_basename.rpartition("." + ext)
            log_wrapper = dot_ext + rest

        if log_file == '':
            log_file = log_basename
            log_wrapper = ''

        h = logging.FileHandler(log_file, "w")
        h.formatter = self.log.handlers.pop().formatter
        lvl = self.log.getEffectiveLevel()
        h.setLevel(lvl)
        self.log.setLevel(logging.NOTSET)  # save debug and others to higher levels.
        self.log.handlers.append(h)
        self.log_wrapper = log_wrapper
        self.log_file = log_file

        self.log.info("Using job log file " + log_file + " with effective level " + logging.getLevelName(lvl))

    def parse_description(self):
        self.init_logging()
        self.modify_queuedata()

    @property
    def state(self):
        return self.__state

    def send_state(self):
        """
        Sends job state to the dedicated panda server.
        """
        if not self.no_update:
            self.log.info("Updating server job status...")
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
            self.log.debug("Got from server: " + _str)
            # jobDesc = json.loads(_str)
            # self.logger.info("Got from server: " % json.dumps(jobDesc, indent=4))

    @state.setter
    def state(self, value):
        """
        Sets new state and updates server.

        :param value: new job state.
        """
        if value != self.__state:
            self.log.info("Setting job state of job %s to %s" % (self.id, value))
            self.__state = value
            self.send_state()

    def run(self):
        self.state = 'starting'
        self.state = 'stagein'

        self.state = 'running'

        args = shlex.split(self.command_parameters, True, True)
        args.insert(0, self.command)

        self.log.info("Starting job cmd: %s" % " ".join(pipes.quote(x) for x in args))

        child = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        child_out = ''
        child_err = ''
        while child.returncode is None:
            out, err = child.communicate()
            child_out += out  # let's assume the output is not very long for now.
            child_err += err

        self.log.info("Job ended with status: %d" % child.returncode)
        self.log.info("Job stdout:\n%s" % child_out)
        self.log.info("Job stderr:\n%s" % child_err)
        self.error_code = child.returncode

        self.state = "holding"
        self.state = 'finished'
