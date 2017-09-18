# Adapted from https://gist.github.com/schlamar/7003737

from __future__ import unicode_literals

import contextlib
import multiprocessing
import logging
import threading


def daemon(log_queue):
    while True:
        try:
            record_data = log_queue.get()
            if record_data is None:
                break
            record = logging.makeLogRecord(record_data)

            logger = logging.getLogger(record.name)
            if logger.isEnabledFor(record.levelno):
                logger.handle(record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except EOFError:
            break
        except:
            logging.exception('Error in log handler.')


class MPLogger(logging.Logger):
    log_queue = None

    def isEnabledFor(self, level):
        return True

    def handle(self, record):
        ei = record.exc_info
        if ei:
            # to get traceback text into record.exc_text
            logging._defaultFormatter.format(record)
            record.exc_info = None  # not needed any more
        d = dict(record.__dict__)
        d['msg'] = record.getMessage()
        d['args'] = None
        self.log_queue.put(d)


def logged_call(log_queue, func, *args, **kwargs):
    MPLogger.log_queue = log_queue
    logging.setLoggerClass(MPLogger)
    # monkey patch root logger and already defined loggers
    logging.root.__class__ = MPLogger
    for logger in logging.Logger.manager.loggerDict.values():
        if not isinstance(logger, logging.PlaceHolder):
            logger.__class__ = MPLogger
    return func(*args, **kwargs)


@contextlib.contextmanager
def open_queue():
    log_queue = multiprocessing.Queue()
    daemon_thread = threading.Thread(target=daemon, args=(log_queue,))
    daemon_thread.daemon = True
    daemon_thread.start()
    yield log_queue
    log_queue.put(None)
