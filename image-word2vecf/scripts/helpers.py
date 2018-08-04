import os
import bz2
import logging
import time
from sys import stdout


def mkdir_if_not_exists(dirname):
    if not os.path.isdir(dirname):
        try:
            os.makedirs(dirname)
        except:
            logging.error('Could not create: %s', dirname)
            return


class JobsReporter:
    def __init__(self, report_period=1, batch_size=1):
        self.finished = 0
        self.report_period = report_period
        self.batch_size = batch_size
        self.start_time = time.time()

    def complete_job(self, num=1, report=False):
        self.finished += num * self.batch_size
        if report == True:
            self.report()

    def report(self):
        if (self.finished % self.report_period == 0):
            elapsed_time = time.time() - self.start_time
            jobs_per_second = self.finished/elapsed_time
            stdout.write('Processing jobs...')
            stdout.write('rate: %8d/s, completed: %10d\r' %
                         (jobs_per_second, self.finished))
            stdout.flush()

    def reset(self):
        self.start_time = time.time()
        self.finished = 0

    def reportElapsed(self):
        elapsed_time = time.time() - self.start_time
        stdout.write('\nElapsed time: {0}'.format(elapsed_time))

# ------------------------------------------------------------------------------
# Output


class NextFile(object):
    """
    Synchronous generation of next available file name.
    """

    filesPerDir = 100

    def __init__(self, path_name):
        self.path_name = path_name
        self.dir_index = -1
        self.file_index = -1

    def __next__(self):
        self.file_index = (self.file_index + 1) % NextFile.filesPerDir
        if self.file_index == 0:
            self.dir_index += 1
        dirname = self._dirname()
        if not os.path.isdir(dirname):
            os.makedirs(dirname)
        return self._filepath()

    next = __next__

    def _dirname(self):
        char1 = self.dir_index % 26
        char2 = self.dir_index // 26 % 26
        return os.path.join(self.path_name, '%c%c' % (ord('A') + char2, ord('A') + char1))

    def _filepath(self):
        return '%s/wiki_%02d' % (self._dirname(), self.file_index)


class OutputSplitter(object):
    """
    File-like object, that splits output to multiple files of a given max size.
    """

    def __init__(self, nextFile, max_file_size=0, compress=True):
        """
        :param nextFile: a NextFile object from which to obtain filenames
            to use.
        :param max_file_size: the maximum size of each file.
        :para compress: whether to write data with bzip compression.
        """
        self.nextFile = nextFile
        self.compress = compress
        self.max_file_size = max_file_size
        self.file = self.open(next(self.nextFile))

    def reserve(self, size):
        if self.file.tell() + size > self.max_file_size:
            self.close()
            self.file = self.open(next(self.nextFile))

    def write(self, data):
        self.reserve(len(data))
        self.file.write(data)

    def close(self):
        self.file.close()

    def open(self, filename):
        if self.compress:
            return bz2.BZ2File(filename + '.bz2', 'w')
        else:
            return open(filename, 'wb')
