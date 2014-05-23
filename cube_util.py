"""
    cube_util.py - utility functions for the datacube.
"""

import os
import subprocess
import time
import pdb
import logging
import pprint

#
# Set up logger
#

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

#
# Utility Functions
#


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
def getFileSizeMB(path):
    """Gets the size of a file (megabytes).

    Arguments:
    path: file path
 
    Returns:
    File size (MB)

    Raises:
    OSError [Errno=2] if file does not exist
    """     
    return os.path.getsize(path) / (1024*1024)

