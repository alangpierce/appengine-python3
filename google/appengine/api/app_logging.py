#!/usr/bin/env python3
#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#





"""Logging utilities for use by applications.

Classes defined here:
  AppLogsHandler: StreamHandler subclass
"""







import logging

from google.appengine import runtime
from google.appengine.api import logservice
from google.appengine.runtime import features




NEWLINE_REPLACEMENT = "\0"

class AppLogsHandler(logging.Handler):
  """Logging handler that will direct output to a persistent store of
  application logs.

  This handler will output log statements to logservice.write(). This handler is
  automatically initialized and attached to the Python common logging library.
  """














  def emit(self, record):
    """Emit a record.

    This implementation is based on the implementation of
    StreamHandler.emit()."""
    try:
      if features.IsEnabled("LogServiceWriteRecord"):
        logservice.write_record(self._AppLogsLevel(record.levelno),
                                record.created,
                                self.format(record))
      else:
        message = self._AppLogsMessage(record)
        logservice.write(message)
    except (KeyboardInterrupt, SystemExit, runtime.DeadlineExceededError):
      raise
    except:
      self.handleError(record)

  def _AppLogsMessage(self, record):
    """Converts the log record into a log line."""



    message = self.format(record).replace("\r\n", NEWLINE_REPLACEMENT)
    message = message.replace("\r", NEWLINE_REPLACEMENT)
    message = message.replace("\n", NEWLINE_REPLACEMENT)

    return "LOG %d %d %s\n" % (self._AppLogsLevel(record.levelno),
                               int(record.created * 1000 * 1000),
                               message)

  def _AppLogsLevel(self, level):
    """Converts the logging level used in Python to the API logging level"""
    if level >= logging.CRITICAL:
      return 4
    elif level >= logging.ERROR:
      return 3
    elif level >= logging.WARNING:
      return 2
    elif level >= logging.INFO:
      return 1
    else:
      return 0
