import threading
import subprocess
import logging
import time

log = logging.getLogger("Utility")


class CollectStream(threading.Thread):
    def __init__(self, stream):
        self.stream = stream
        self.buffer = ''

    def run(self):
        while True:
            out = self.stream.readline()
            if out:
                self.buffer += out
            else:
                break

class GetOutput(threading.Thread):
    def __init__(self, child):
        threading.Thread.__init__(self)
        self.child = child
        self.out = ''
        self.err = ''

    def run(self):
        child = self.child
        o_iterator = iter(child.stdout.readline, b"")
        e_iterator = iter(child.stderr.readline, b"")

        while child.poll() is None:
            for line in o_iterator:
                print(line)
                self.out += line
            for line in e_iterator:
                print(line)
                self.err += line


class Utility(object):

    def __init__(self):
        pass

    def call(self, arguments, timeout=None, terminate_timeout=5):
        child = subprocess.Popen(arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1)

        o = GetOutput(child)

        o.start()

        o.join(timeout)
        if o.is_alive:
            log.info("child timed out, terminating")
            child.terminate()
            o.join(terminate_timeout)
            if o.is_alive:
                log.info("child termination timed out, killing")
                child.kill()
                o.join(terminate_timeout)

        return child.returncode, o.out, o.err


if __name__ == "__main__":
    u=Utility()
    logging.basicConfig()
    log.setLevel(logging.DEBUG)
    c,o,e = u.call(["bash","trap.sh"],timeout=1,terminate_timeout=1)
    print("%d\n____________________\n"%c)
    print(o)
    print("\n____________________\n")
    print(e)
    print("\n____________________\n")
