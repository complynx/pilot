import urllib
import os
import commands
import json
from job_description_fixer import description_fixer
import shlex
import pipes


class Job(object):
    __state = "sent"
    pilot = None
    description = None
    error_code = None
    no_update = False
    description_aliases = {
        'id': 'job_id'
    }

    def __init__(self, _pilot, _desc):
        self.pilot = _pilot
        if _pilot.args.no_job_update:
            self.no_update = True
        self.description = description_fixer(_desc)
        self.pilot.logger.debug(json.dumps(self.description, indent=4, sort_keys=True))
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

    def modify_queuedata(self):
        params = self.command_parameters
        modifier = ""
        others = ""
        if isinstance(params, basestring):
            splat = shlex.split(params, True)
            for key in splat:
                if key.startswith('--overwriteQueuedata='):
                    modifier = key[len("--overwriteQueuedata="):]
                else:
                    others += " " + pipes.quote(key)

            self.pilot.logger.debug("overwrite: %s" % (modifier))
            self.pilot.logger.debug("params: %s" % (others))

    def parse_description(self):
        self.modify_queuedata()
        self.pilot.logger.debug("id: %d" % self.id)

    @property
    def state(self):
        return self.__state

    def send_state(self):
        """
        Sends job state to the dedicated panda server.
        """
        if not self.no_update:
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
            self.pilot.logger.info("Setting job state of job %s to %s" % (self.id, value))
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
