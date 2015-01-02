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
"""Stub implementations of restricted functions."""



import errno
import functools
import inspect
import io
import locale
import mimetypes
import os
import random
import re
import sys
import threading

# sysconfig is new in Python 2.7.
try:
  import sysconfig
except ImportError:
  sysconfig = None


def os_error_not_implemented(*unused_args, **unused_kwargs):
  raise OSError(errno.ENOSYS, 'Function not implemented')


def return_minus_one(*unused_args, **unused_kwargs):
  return -1


def fake_uname():
  """Fake version of os.uname."""
  return ('Linux', '', '', '', '')


def fake_set_locale(category, value=None, original_setlocale=locale.setlocale):
  """Fake version of locale.setlocale that only supports the default."""
  if value not in (None, '', 'C', 'POSIX'):
    raise locale.Error('locale emulation only supports "C" locale')
  return original_setlocale(category, 'C')


def fake_get_platform():
  """Fake distutils.util.get_platform()."""
  if sys.platform == 'darwin':
    return 'macosx-'
  else:
    return 'linux-'
