from .TqdmLoggingHandler import TqdmLoggingHandler
import logging

class CustomLogger():
  def __init__(self, logger_target,  warning_logs=[], error_logs=[], critical_logs=[]):
    self.logger = self.set_logger(logger_target, warning_logs, error_logs, critical_logs)

  def set_logger(self, logger_target, warning_logs, error_logs, critical_logs):
    try:
      formatter = logging.Formatter("%(asctime)s -- %(message)s")
      for elem in warning_logs:
        logging.getLogger(elem).setLevel(logging.WARNING)
      for elem in error_logs:
        logging.getLogger(elem).setLevel(logging.ERROR)
      for elem in critical_logs:
        logging.getLogger(elem).setLevel(logging.CRITICAL)
      log = logging.getLogger(logger_target)
      log.setLevel(logging.INFO)
      handler = TqdmLoggingHandler()
      handler.setFormatter(formatter)
      log.addHandler(handler)
      return log
    except (KeyboardInterrupt, SystemExit):
      raise
    except:
      self.handleError(record)