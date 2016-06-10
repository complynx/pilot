import urllib
import os
import subprocess
import json
import shlex
import pipes
import re
import logging
from utility import Utility


class LoggingContext(object):
    """
    Class to override logging level for specified handler.
    Used for output header and footer of log file regardless the level.
    Automatically resets level on exit.

    Usage:

        with LoggingContext(handler, new_level):
            log.something

    """
    def __init__(self, handler, level=None):
        self.level = level
        self.handler = handler

    def __enter__(self):
        if self.level is not None:
            self.old_level = self.handler.level
            self.handler.setLevel(self.level)

    def __exit__(self, et, ev, tb):
        if self.level is not None:
            self.handler.setLevel(self.old_level)


class Job(Utility):
    """
    This class holds a job and helps with it.
    Class presents also an interface to job description. Each field in it is mirrored to this class if there is no other
    specific variable, that shadows it.
    For example, job_id will be mirrored, but log_file would not, because there is specific class instance property,
    shadowing it.
    Every shadowing property should have exactly the same meaning and a great reason to shadow the one from description.
    Every such property must be documented.

    Attributes:
        id                      Alias to job_id
        state                   Job last state
        pilot                   Link to Pilot class instance
        description             Job description
        error_code              Job payload exit code
        no_update               Flag, specifying whether we will update server
        log_file                Job dedicated log file, into which the logs _are_ written. Shadowing log_file from
                                description, because that file is not a log file, but an archive containing it.
                                Moreover, log_file from description may contain not only log file.
                                :Shadowing property:
        log_archive             Detected archive extension. Mostly ".tgz"
        log                     Logger, used by class members.
        log_handler             File handler of real log file for logging. Added to root logger to catch outer calls.
        log_level               Filter used by handler to filter out unnecessary logging data.
                                Acquired from ''pilot.jobmanager'' logger configuration.
                                :Static:
        log_formatter           Formatter used by log handlers.
                                Acquired from ''pilot.jobmanager'' logger configuration.
                                :Static:
    """
    pilot = None
    description = None
    error_code = None
    no_update = False
    log_file = 'stub.job.log'
    log_archive = '.tgz'
    log = logging.getLogger()
    log_handler = None
    log_level = None
    log_formatter = None

    __state = "sent"
    __description_aliases = {
        'id': 'job_id'
    }
    __acceptable_log_wrappers = ["tar", "tgz", "gz", "gzip", "tbz2", "bz2", "bzip2"]

    def __init__(self, _pilot, _desc):
        Utility.__init__(self)
        self.log = logging.getLogger('pilot.jobmanager')
        self.pilot = _pilot
        if _pilot.args.no_job_update:
            self.no_update = True
        self.description = _desc
        _pilot.logger.debug(json.dumps(self.description, indent=4, sort_keys=True))
        self.parse_description()

    def __getattr__(self, item):
        """
        Propagation of description values to Job instance if they are not shadowed.
        """
        try:
            return object.__getattribute__(self, item)
        except AttributeError:
            if self.description is not None:
                if item in self.__description_aliases:
                    return self.description[self.__description_aliases[item]]
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
                if key in self.__description_aliases:
                    self.description[self.__description_aliases[key]] = value
                elif self.description is not None and key in self.description:
                    self.description[key] = value
                return
            object.__setattr__(self, key, value)

    def extract_queuedata_updates(self, job_parameters):
        """
        Extracts the queuedata-overwrite key=value pairs from the job parameters.
        Port from previous version, because this thing is not posix-compliant.
        Also it works awfully unpredictable, so I don't lke the whole of it.
        Ported _only_ for back-compatibility of job descriptions.
        Must be reworked.
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
                    self.log.info("Detected quotation marks in the job parameters: %s" % (pairs[0]))
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
                    self.log.info("Extracted the following key value pairs from job parameters: %s" % str(kv_ist))

                    for key, value in kv_ist:

                        if key != "":
                            # extract the value from the comma_dictionary if it exists
                            if value in comma_dictionary:
                                value = comma_dictionary[value]

                            queuedata_update_dict[key] = value
                        else:
                            self.log.warning("Bad key detected in key value tuple: %s" % str((key, value)))
                else:
                    self.log.warning("Failed to extract the key value pair list from: %s" % (pairs[0]))
            else:
                self.log.warning("Failed to extract the key value pairs from: %s" % (pairs[0]))
        else:
            self.log.warning("Failed to extract the full queuedata overwrite command from jobParameters=%s" %
                             job_parameters)

        return job_parameters, queuedata_update_dict

    def modify_queuedata(self):
        """
        Finds and parses parameters to be overwritten, then modifies queuedata accordingly.
        """
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
        """
        Sets up logger handler for specified job log file. Beforehand it extracts job log file's real name and it's
        archive extension.
        """
        log_basename = self.description["log_file"]

        log_file = ''
        log_archive = ''

        for ext in self.__acceptable_log_wrappers:
            if log_file != '':
                break
            log_file, dot_ext, rest = log_basename.rpartition("." + ext)
            log_archive = dot_ext + rest

        if log_file == '':
            log_file = log_basename
            log_archive = ''

        h = logging.FileHandler(log_file, "w")
        if Job.log_level is None:
            Job.log_formatter = self.log.handlers.pop().formatter
            lvl = self.log.getEffectiveLevel()
            Job.log_level = lvl
        else:
            lvl = Job.log_level

        h.formatter = Job.log_formatter

        if lvl > logging.NOTSET:
            h.setLevel(lvl)

        self.log.setLevel(logging.NOTSET)  # save debug and others to higher levels.

        root_log = logging.getLogger()
        root_log.handlers.append(h)

        self.log_archive = log_archive
        self.log_file = log_file
        self.log_handler = h

        with LoggingContext(h, logging.NOTSET):
            self.log.info("Using job log file " + self.log_file)
            self.pilot.print_initial_information()
            self.log.info("Using effective log level " + logging.getLevelName(lvl))

    def parse_description(self):
        """
        Initializes description induced configurations: log handlers, queuedata modifications, etc.
        """
        self.init_logging()
        self.modify_queuedata()

    @property
    def state(self):
        """
        :return: Last job state
        """
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

    def prepare_log(self, include_files=None):
        """
        Prepares log file for stage out.
         May be called several times. The prime log file is not removed, so it will append new information (may be
         useful on log stage out failures to append the info).
         Automatically detects tarball and zipping based on previously extracted log archive extension.

        :param include_files: array of files to be included if tarball is used to aggregate log.
        """
        with LoggingContext(self.log_handler, logging.NOTSET):
            import shutil
            full_log_name = self.log_file + self.log_archive

            self.log.info("Preparing log file to send.")

            if os.path.isfile(full_log_name) and self.log_file != full_log_name:
                os.remove(full_log_name)

            mode = "w"
            if self.log_archive.find("g") >= 0:
                self.log.info("Detected compression gzip.")
                mode += ":gz"
                from gzip import open as compressor
            elif self.log_archive.find("2") >= 0:
                self.log.info("Detected compression bzip2.")
                mode += ":bz2"
                from bz2 import BZ2File as compressor

            if self.log_archive.find("t") >= 0:
                self.log.info("Detected log archive: tar.")
                import tarfile

                with tarfile.open(full_log_name, mode) as tar:
                    if include_files is not None:
                        for f in include_files:
                            if os.path.exists(f):
                                self.log.info("Adding file %s" % f)
                                tar.add(f)
                    self.log.info("Adding log file... (must be end of log)")
                    tar.add(self.log_file)

                self.log.info("Finalizing log file.")
                tar.close()

            elif mode != "w":  # compressor
                self.log.info("Compressing log file... (must be end of log)")
                with open(self.log_file, 'rb') as f_in, compressor(full_log_name, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            elif self.log_file != full_log_name:
                self.log.warn("Compression is not known, assuming no compression.")
                self.log.info("Copying log file... (must be end of log)")

                shutil.copyfile(self.log_file, full_log_name)

        self.log.info("Log file prepared for stageout.")

    def rucio_info(self):
        c,o,e = self.call(['rucio', 'whoami'])
        self.log.info("Rucio whoami responce: \n" + o)
        if e != '':
            self.log.warn("Rucio returned error(s): \n" + e)

    def stage_in(self):
        self.state = 'stagein'
        self.rucio_info()
        for f in self.input_files:
            c,o,e = self.call(['rucio', 'download', '--no-subdir', self.input_files[f]['scope'] + ":" + f])

    def payload_run(self):
        self.state = 'running'
        args = shlex.split(self.command_parameters, True, True)
        args.insert(0, self.command)

        self.log.info("Starting job cmd: %s" % " ".join(pipes.quote(x) for x in args))

        c,o,e = self.call(args)

        self.log.info("Job ended with status: %d" % c)
        self.log.info("Job stdout:\n%s" % o)
        self.log.info("Job stderr:\n%s" % e)
        self.error_code = c

        self.state = "holding"

    def run(self):
        """
        Main code of job manager.

        Stages in, executes and stages out the job.
        """
        self.state = 'starting'

        self.stage_in()
        self.payload_run()

        self.prepare_log()
        self.state = 'finished'
