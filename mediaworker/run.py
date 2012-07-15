#!/usr/bin/env python
import os.path
import time
from multiprocessing import Process, Queue
from glob import glob
import logging
from logging.handlers import RotatingFileHandler
import sys
import signal
import traceback

from mediaworker import settings


WORKERS_DIR = 'workers'


logger = logging.getLogger(__name__)


class QueueHandler(logging.Handler):
    '''Logging handler which sends events to a multiprocessing queue.
    '''
    def __init__(self, queue):
        logging.Handler.__init__(self)
        self.queue = queue

    def emit(self, record):
        '''Emit a record.
        Writes the LogRecord to the queue.
        '''
        try:
            ei = record.exc_info
            if ei:
                dummy = self.format(record) # just to get traceback text into record.exc_text
                record.exc_info = None  # not needed any more
            self.queue.put_nowait(record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


def listener_configurer():
    formatter = logging.Formatter(settings.LOG_FORMAT)
    root = logging.getLogger()

    # Standard file logging
    fh = RotatingFileHandler(settings.LOG_DEFAULT, 'a', settings.LOG_SIZE, settings.LOG_COUNT)
    fh.setFormatter(formatter)
    root.addHandler(fh)

    # Errors file logging
    eh = RotatingFileHandler(settings.LOG_ERRORS, 'a', settings.LOG_SIZE, settings.LOG_COUNT)
    eh.setFormatter(formatter)
    eh.setLevel(logging.ERROR)
    root.addHandler(eh)

def listener_process(queue, configurer):
    configurer()
    while True:
        try:
            record = queue.get()
            if record is None:  # we send this as a sentinel to tell the listener to quit
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            traceback.print_exc(file=sys.stderr)

def worker_configurer(queue):
    handler = QueueHandler(queue)
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)

def worker_process(queue, configurer, callable):
    configurer(queue)
    callable()

def get_workers():
    workers = {}
    workers_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), WORKERS_DIR)
    for file in glob(workers_path + '/*.py'):
        worker = os.path.splitext(os.path.basename(file))[0]
        try:
            module = __import__('%s.%s' % (WORKERS_DIR, worker), globals(), locals(), [worker], -1)
        except Exception:
            logger.error('failed to import module %s.%s', WORKERS_DIR, worker)
            continue

        target = getattr(module, 'main', None)
        if target:
            workers[worker] = {'target': target}
    return workers

def main():
    os.setpgrp()

    def sigint_handler(signum, frame):
        os.killpg(os.getpgrp(), 9)

    signal.signal(signal.SIGTERM, sigint_handler)

    queue = Queue(-1)
    listener = Process(target=listener_process,
            args=(queue, listener_configurer))
    listener.start()

    workers = get_workers()

    while True:
        for worker in workers:
            proc = workers[worker].get('proc')
            if proc:
                if proc.is_alive():
                    continue
                logger.error('%s died', worker)

            # Start worker
            workers[worker]['proc'] = Process(target=worker_process,
                    args=(queue, worker_configurer, workers[worker]['target']),
                    name=worker)
            workers[worker]['proc'].start()
            logger.info('started %s', worker)
            time.sleep(1)

        time.sleep(10)


if __name__ == '__main__':
    main()
