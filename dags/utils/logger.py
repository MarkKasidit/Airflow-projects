import os
import logging
import sys

class Logger(object):

    def __init__(self, name):
        """ a Logger can be used to capture diagnostic information (to a file)

        :param name: the name of the logger (and also the name of the log file
                     with a .log extension)
        :return: nothing
        """
        name = name.replace('.log', '')
        # log_namespace can be replaced with your namespace
        logger = logging.getLogger('dp-postgrest-loader.%s' % name)
        logger.setLevel(logging.DEBUG)
        if not logger.handlers:
            formatter = logging.Formatter(
                '%(asctime)s %(levelname)s:%(name)s %(message)s'
            )

            # for ease of use, also add a handler that outputs to the screen
            ch = logging.StreamHandler(sys.stdout)
            ch.setLevel(logging.DEBUG)
            ch.setFormatter(formatter)
            logger.addHandler(ch)
        self._logger = logger

    def get(self):
        """
        :return: returns the singleton logger
        """
        return self._logger