"""
    cube_util.py - utility functions for the datacube.
"""

import os
import subprocess
import time
import datetime
import pdb
import logging
import pprint
import errno
import inspect

#
# Set up logger
#

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

#
# Utility Functions
#


def get_datacube_root():
    """Return the directory containing the datacube python source files.

    This returns the value of the DATACUBE_ROOT environment variable
    if it is set, otherwise it returns the directory containing the
    source code for this function (cube_util.get_datacube_root).
    """

    try:
        datacube_root = os.environ['DATACUBE_ROOT']
    except KeyError:
        this_file = inspect.getsourcefile(get_datacube_root)
        datacube_root = os.path.dirname(os.path.abspath(this_file))

    return datacube_root


def execute_python(command_string, command_dir=None, working_dir=None):
    """Execute a python command as if from the command line.

    This runs a python script in a subprocess.

    Arguments:
        command_string: A string holding the command, eg
            'landsat_ingester.py --source /g/data1/some_test_dir --debug'

        command_dir: The directory containing the script to execute. The
            default is datacube_root as obtained by get_datacube_root()
            above. This will not be used if the name of the script seems
            to be an absolute path, otherwise it will be used as a prefix
            to the command name.

        working_dir: The working directory for the command. This can be
            relative (to the current directory) or absolute. The current
            directory will be restored after the command has been executed.
            The default is the current directory.

    Return Value:
        Returns a dictionary as follows:
            { 'command': <str>,
              'command_list': <list>,
              'returncode': <int>,
              'stdout': <stdout text>,
              'stderr': <stderr text>,
              'elapsed_time': <time in seconds>,
              'cpu_time': <time in seconds>
              }

    """

    command_list = command_string.split()
    if command_dir is None:
        command_dir = get_datacube_root()
    if not os.path.isabs(command_list[0]):
        command_list[0] = os.path.join(command_dir, command_list[0])
    command_list = ['python'] + command_list

    new_wd = working_dir if working_dir is not None else os.getcwd()

    result = {}
    result['command'] = command_string
    result['command_list'] = command_list

    sw = Stopwatch()

    sw.start()
    p = subprocess.Popen(command_list,
                         cwd=new_wd,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE
                         )
    sw.stop()

    (result['stdout'], result['stderr']) = p.communicate()
    result['returncode'] = p.returncode
    (result['elapsed_time'], result['cpu_time']) = sw.read()

    return result


def parse_date_from_string(date_string):
    """Attempt to parse a date from a command line or config file argument.

    This function tries a series of date formats, and returns a date
    object if one of them works, None otherwise.
    """

    format_list = ['%Y%m%d',
                   '%d/%m/%Y',
                   '%Y-%m-%d'
                   ]

    # Try the formats in the order listed.
    date = None
    for date_format in format_list:
        try:
            date = datetime.datetime.strptime(date_string, date_format).date()
            break
        except ValueError:
            pass

    return date

#
# log_multiline utility function copied from ULA3
#


def log_multiline(log_function, log_text, title=None, prefix=''):
    """Function to log multi-line text.

    This is a clone of the log_multiline function from the ULA3 package.
    It is repeated here to reduce cross-repository dependancies.
    """

    LOGGER.debug('log_multiline(%s, %s, %s, %s) called',
                 log_function, repr(log_text), repr(title), repr(prefix))

    if type(log_text) == str:
        LOGGER.debug('log_text is type str')
        log_list = log_text.splitlines()
    elif type(log_text) == list and type(log_text[0]) == str:
        LOGGER.debug('log_text is type list with first element of type text')
        log_list = log_text
    else:
        LOGGER.debug('log_text is type ' + type(log_text).__name__)
        log_list = pprint.pformat(log_text).splitlines()

    log_function(prefix + '=' * 80)
    if title:
        log_function(prefix + title)
        log_function(prefix + '-' * 80)

    for line in log_list:
        log_function(prefix + line)

    log_function(prefix + '=' * 80)


#
# execute utility function copied from ULA3
#
# pylint: disable = too-many-arguments, too-many-locals
#
# The extra arguments are keyword arguments passed along to
# subprocess. This seems resonable.
#


def execute(command_string=None, shell=True, cwd=None, env=None,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            preexec_fn=None, close_fds=False, bufsize=-1,
            debug=False):
    """
    Executes a command as a subprocess.

    This function is a thin wrapper around :py:func:`subprocess.Popen` that
    gathers some extra information on the subprocess's execution context
    and status.  All arguments except 'debug' are passed through
    to :py:func:`subprocess.Popen` as-is.

    :param command_string:
        Commands to be executed.

    :param shell:
        Execute via the shell

    :param cwd:
        Working directory for the subprocess

    :param env:
        Environment for the subprocess

    :param stdout:
        stdout for the subprocess

    :param stderr:
        stdout for the subprocess

    :param close_fds:
        close open file descriptors before execution

    :param bufsize:
        buffer size

    :param debug:
        debug flag

    :return:
        Dictionary containing command, execution context and status:
            { 'command': <str>,
            'returncode': <int>,
            'pid': <int>,
            'stdout': <stdout text>,
            'stderr': <stderr text>,
            'caller_ed': <caller working directory>,
            'env': <execution environment>,}

    :seealso:
        :py:func:`subprocess.Popen`
    """

    assert command_string
    parent_wd = os.getcwd()

    p = subprocess.Popen(command_string,
                         shell=shell,
                         cwd=cwd,
                         env=env,
                         stdout=stdout,
                         stderr=stderr,
                         bufsize=bufsize,
                         close_fds=close_fds,
                         preexec_fn=preexec_fn,
                         )
    start_time = time.time()
    out, err = p.communicate()
    result = {'command': command_string,
              'returncode': p.returncode,
              'pid': p.pid,
              'stdout': out,
              'stderr': err,
              'parent_wd': parent_wd,
              'cwd': cwd,
              'env': env,
              'elapsed_time': time.time() - start_time,
              }

    if debug:
        print '\n*** DEBUG ***'
        print 'sub_process.execute'
        pprint.pprint(result)
        pdb.set_trace()

    return result

# pylint: enable = too-many-arguments, too-many-locals
def get_file_size_mb(path):
    """Gets the size of a file (megabytes).

    Arguments:
    path: file path

    Returns:
    File size (MB)

    Raises:
    OSError [Errno=2] if file does not exist
    """
    return os.path.getsize(path) / (1024*1024)

def create_directory(dirname):
    """Create dirname, including any intermediate directories necessary to
    create the leaf directory."""
    # Allow group permissions on the directory we are about to create
    old_umask = os.umask(0o007)
    try:
        os.makedirs(dirname)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise DatasetError('Directory %s could not be created' %dirname)
    finally:
        # Put back the old umask
        os.umask(old_umask)

#
# Utility classes
#

class Stopwatch(object):
    """Timer for simple performance measurements."""

    def __init__(self):
        """Initial state."""
        self.elapsed_time = 0.0
        self.cpu_time = 0.0
        self.start_elapsed_time = None
        self.start_cpu_time = None
        self.running = False

    def start(self):
        """Start the stopwatch."""
        if not self.running:
            self.start_elapsed_time = time.time()
            self.start_cpu_time = time.clock()
            self.running = True

    def stop(self):
        """Stop the stopwatch."""
        if self.running:
            self.elapsed_time += (time.time() - self.start_elapsed_time)
            self.cpu_time += (time.clock() - self.start_cpu_time)
            self.start_elapsed_time = None
            self.start_cpu_time = None
            self.running = False

    def reset(self):
        """Reset the stopwatch."""
        self.__init__()

    def read(self):
        """Read the stopwatch. Returns a tuple (elapsed_time, cpu_time)."""

        if self.running:
            curr_time = time.time()
            curr_clock = time.clock()

            self.elapsed_time += (curr_time - self.start_elapsed_time)
            self.cpu_time += (curr_clock - self.start_cpu_time)
            self.start_elapsed_time = curr_time
            self.start_cpu_time = curr_clock

        return (self.elapsed_time, self.cpu_time)

#
# Exceptions
#


class DatasetError(Exception):
    """
    A problem specific to a dataset. If raised it will cause the
    current dataset to be skipped, but the ingest process will continue.
    """

    pass
