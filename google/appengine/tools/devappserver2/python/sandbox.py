#!/usr/bin/env python
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
"""A sandbox implementation that emulates production App Engine."""



import builtins
import imp
import os
import re
import sys
import traceback
import types

import google

from google.appengine import dist
from google.appengine.api import app_logging
from google.appengine.api.logservice import logservice
from google.appengine import dist27 as dist27
from google.appengine.ext.remote_api import remote_api_stub
from google.appengine.runtime import request_environment
from google.appengine.tools.devappserver2.python import pdb_sandbox
from google.appengine.tools.devappserver2.python import request_state
from google.appengine.tools.devappserver2.python import stubs

# Needed to handle source file encoding
CODING_MAGIC_COMMENT_RE = re.compile('coding[:=]\s*([-\w.]+)')
DEFAULT_ENCODING = 'ascii'

_C_MODULES = frozenset(['cv', 'Crypto', 'lxml', 'numpy', 'PIL'])

NAME_TO_CMODULE_WHITELIST_REGEX = {
    'cv': re.compile(r'cv(\..*)?$'),
    'lxml': re.compile(r'lxml(\..*)?$'),
    'numpy': re.compile(r'numpy(\..*)?$'),
    'pycrypto': re.compile(r'Crypto(\..*)?$'),
    'PIL': re.compile(r'(PIL(\..*)?|_imaging|_imagingft|_imagingmath)$'),
    'ssl': re.compile(r'_ssl$'),
}

# Maps App Engine third-party library names to the Python package name for
# libraries whose names differ from the package names.
_THIRD_PARTY_LIBRARY_NAME_OVERRIDES = {
    'pycrypto': 'Crypto',
}

# The location of third-party libraries will be different for the packaged SDK.
_THIRD_PARTY_LIBRARY_FORMAT_STRING = (
    'lib/%(name)s-%(version)s')

# Store all the modules removed from sys.modules so they don't get cleaned up.
_removed_modules = []

_open_hooks = []


def add_open_hook(install_open_hook):
  """Hook the open chain to allow files to be opened from FS-like containers.

  In order to allow files to be opened from FS-like containers such as zip
  files, provide a sandbox compatible way to hook into the open chain. To
  correctly work with our sandbox, these hooks must be called before FakeFile.
  Due to code flow, the easiest way to allow that is for code to provide an
  install function that the sandbox calls at the appropriate time.

  Hook functions are expected to only handle paths that cannot be handled by
  the standard filesystem open and are expected to forward all other paths
  to the next hook. Hook functions are responsible for saving the next hook
  function by getting the value of __builtin__.open when the install function
  is called (very key point here, make sure to evaluate __builtin__.open when
  your install function is called and not at import time).

  Args:
    install_open_hook: a method of no parameters that will install an open
      hook.
  """
  _open_hooks.append(install_open_hook)


def _make_request_id_aware_start_new_thread(base_start_new_thread):
  """Returns a replacement for start_new_thread that inherits request id.

  Returns a function with an interface that matches thread.start_new_thread
  where the new thread inherits the request id of the current thread. The
  request id is used by the Remote API to associate API calls with the HTTP
  request that provoked them.

  Args:
    base_start_new_thread: The thread.start_new_thread function to call to
        create a new thread.

  Returns:
    A replacement for start_new_thread.
  """

  def _start_new_thread(target, args, kw=None):
    if kw is None:
      kw = {}

    request_id = remote_api_stub.RemoteStub._GetRequestId()
    request = request_state.get_request_state(request_id)

    def _run():
      try:
        remote_api_stub.RemoteStub._SetRequestId(request_id)
        request.start_thread()
        target(*args, **kw)
      finally:
        request_environment.current_request.Clear()
        request.end_thread()
    return base_start_new_thread(_run, ())
  return _start_new_thread


def enable_sandbox(config):
  """Enable the sandbox based on the configuration.

  This includes installing import hooks to restrict access to C modules and
  stub out functions that are not implemented in production, replacing the file
  builtins with read-only versions and add enabled libraries to the path.

  Args:
    config: The runtime_config_pb2.Config to use to configure the sandbox.
  """









  devnull = open(os.path.devnull)
  modules = [os, traceback, google]
  c_module = _find_shared_object_c_module()
  if c_module:
    modules.append(c_module)
  module_paths = [module.__file__ for module in modules]
  module_paths.extend([os.path.realpath(module.__file__) for module in modules])
  app_root = config.application_root.decode()
  python_lib_paths = [app_root]
  for path in sys.path:
    if any(module_path.startswith(path) for module_path in module_paths):
      python_lib_paths.append(path)
  python_lib_paths.extend(_enable_libraries(config.libraries))
  for name in list(sys.modules):
    if not _should_keep_module(name):
      _removed_modules.append(sys.modules[name])
      del sys.modules[name]
  sys.platform = 'linux3'
  sys.meta_path = [
      PyCryptoRandomImportHook,
      ] + sys.meta_path
  sys.path_importer_cache = {}
  sys.path = python_lib_paths[:]

  thread = __import__('_thread')
  __import__('%s.threading' % dist27.__name__)
  threading = sys.modules['%s.threading' % dist27.__name__]
  thread.start_new_thread = _make_request_id_aware_start_new_thread(
      thread.start_new_thread)
  # This import needs to be after enabling the sandbox so it imports the
  # sandboxed version of the logging module.
  from google.appengine.runtime import runtime
  runtime.PatchStartNewThread(thread)
  threading._start_new_thread = thread.start_new_thread

  os.chdir(app_root)
  sandboxed_os = __import__('os')
  request_environment.PatchOsEnviron(sandboxed_os)
  os.__dict__.update(sandboxed_os.__dict__)
  _init_logging(config.stderr_log_level)
  pdb_sandbox.install(config)
  sys.stdin = devnull
  sys.stdout = sys.stderr


def _find_shared_object_c_module():
  for module_name in ['_sqlite3', '_multiprocessing', '_ctypes', 'bz2']:
    try:
      module = __import__(module_name)
    except ImportError:
      continue
    else:
      if hasattr(module, '__file__'):
        return module
  return None


def _should_keep_module(name):
  """Returns True if the module should be retained after sandboxing."""
  return (name in ('__builtin__', 'sys', 'codecs', 'encodings', 'site',
                   'google') or
          name.startswith('google.') or name.startswith('encodings.') or




          # Making mysql available is a hack to make the CloudSQL functionality
          # work.
          'mysql' in name.lower())


def _init_logging(stderr_log_level):
  logging = __import__('logging')
  logger = logging.getLogger()

  console_handler = logging.StreamHandler(sys.stderr)
  if stderr_log_level == 0:
    console_handler.setLevel(logging.DEBUG)
  elif stderr_log_level == 1:
    console_handler.setLevel(logging.INFO)
  elif stderr_log_level == 2:
    console_handler.setLevel(logging.WARNING)
  elif stderr_log_level == 3:
    console_handler.setLevel(logging.ERROR)
  elif stderr_log_level == 4:
    console_handler.setLevel(logging.CRITICAL)

  console_handler.setFormatter(logging.Formatter(
      '%(levelname)-8s %(asctime)s %(filename)s:%(lineno)s] %(message)s'))
  logger.addHandler(console_handler)

  logging_stream = request_environment.RequestLocalStream(
      request_environment.current_request)
  logger.addHandler(app_logging.AppLogsHandler())
  logger.setLevel(logging.DEBUG)
  logservice.logs_buffer = lambda: request_environment.current_request.errors
  sys.stderr = Tee(sys.stderr, logging_stream)


class Tee(object):
  """A writeable stream that forwards to zero or more streams."""

  def __init__(self, *streams):
    self._streams = streams

  def close(self):
    for stream in self._streams:
      stream.close()

  def flush(self):
    for stream in self._streams:
      stream.flush()

  def write(self, data):
    for stream in self._streams:
      stream.write(data)

  def writelines(self, data):
    for stream in self._streams:
      stream.writelines(data)


def _enable_libraries(libraries):
  """Add enabled libraries to the path.

  Args:
    libraries: A repeated Config.Library containing the libraries to enable.

  Returns:
    A list of paths containing the enabled libraries.
  """
  library_dirs = []
  library_pattern = os.path.join(os.path.dirname(
      os.path.dirname(google.__file__)), _THIRD_PARTY_LIBRARY_FORMAT_STRING)
  for library in libraries:
    # Encode the library name/version to convert the Python type
    # from unicode to str so that Python doesn't try to decode
    # library pattern from str to unicode (which can cause problems
    # when the SDK has non-ASCII data in the directory). Encode as
    # ASCII should be safe as we control library info and are not
    # likely to have non-ASCII names/versions.
    library_dir = os.path.abspath(
        library_pattern % {'name': library.name.encode('ascii'),
                           'version': library.version.encode('ascii')})
    library_dirs.append(library_dir)
  return library_dirs


class BaseImportHook(object):
  """A base class implementing common import hook functionality.

  This provides utilities for implementing both the finder and loader parts of
  the PEP 302 importer protocol and implements the optional extensions to the
  importer protocol.
  """

  def _find_module_or_loader(self, submodule_name, fullname, path):
    """Acts like imp.find_module with support for path hooks.

    Args:
      submodule_name: The name of the submodule within its parent package.
      fullname: The full name of the module to load.
      path: A list containing the paths to search for the module.

    Returns:
      A tuple (source_file, path_name, description, loader) where:
        source_file: An open file or None.
        path_name: A str containing the path to the module.
        description: A description tuple like the one imp.find_module returns.
        loader: A PEP 302 compatible path hook. If this is not None, then the
            other elements will be None.

    Raises:
      ImportError: The module could not be imported.
    """
    for path_entry in path + [None]:
      result = self._find_path_hook(submodule_name, fullname, path_entry)
      if result is not None:
        break
    else:
      raise ImportError('No module named %s' % fullname)
    if isinstance(result, tuple):
      return result + (None,)
    else:
      return None, None, None, result

  def _find_and_load_module(self, submodule_name, fullname, path):
    """Finds and loads a module, using a provided search path.

    Args:
      submodule_name: The name of the submodule within its parent package.
      fullname: The full name of the module to load.
      path: A list containing the paths to search for the module.

    Returns:
      The requested module.

    Raises:
      ImportError: The module could not be imported.
    """
    source_file, path_name, description, loader = self._find_module_or_loader(
        submodule_name, fullname, path)
    if loader:
      return loader.load_module(fullname)
    try:
      return imp.load_module(fullname, source_file, path_name, description)
    finally:
      if source_file:
        source_file.close()

  def _find_path_hook(self, submodule, submodule_fullname, path_entry):
    """Helper for _find_and_load_module to find a module in a path entry.

    Args:
      submodule: The last portion of the module name from submodule_fullname.
      submodule_fullname: The full name of the module to be imported.
      path_entry: A single sys.path entry, or None representing the builtins.

    Returns:
      None if nothing was found, a PEP 302 loader if one was found or a
      tuple (source_file, path_name, description) where:
          source_file: An open file of the source file.
          path_name: A str containing the path to the source file.
          description: A description tuple to be passed to imp.load_module.
    """
    if path_entry is None:
      # This is the magic entry that tells us to look for a built-in module.
      if submodule_fullname in sys.builtin_module_names:
        try:
          result = imp.find_module(submodule)
        except ImportError:
          pass
        else:
          # Did find_module() find a built-in module?  Unpack the result.
          _, _, description = result
          _, _, file_type = description
          if file_type == imp.C_BUILTIN:
            return result
      # Skip over this entry if we get this far.
      return None

    # It's a regular sys.path entry.
    try:
      importer = sys.path_importer_cache[path_entry]
    except KeyError:
      # Cache miss; try each path hook in turn.
      importer = None
      for hook in sys.path_hooks:
        try:
          importer = hook(path_entry)
          # Success.
          break
        except ImportError:
          # This importer doesn't handle this path entry.
          pass
      # Cache the result, whether an importer matched or not.
      sys.path_importer_cache[path_entry] = importer

    if importer is None:
      # No importer.  Use the default approach.
      try:
        return imp.find_module(submodule, [path_entry])
      except ImportError:
        pass
    else:
      # Have an importer.  Try it.
      loader = importer.find_module(submodule_fullname)
      if loader is not None:
        # This importer knows about this module.
        return loader

    # None of the above.
    return None

  def _get_parent_package(self, fullname):
    """Retrieves the parent package of a fully qualified module name.

    Args:
      fullname: Full name of the module whose parent should be retrieved (e.g.,
        foo.bar).

    Returns:
      Module instance for the parent or None if there is no parent module.

    Raises:
      ImportError: The module's parent could not be found.
    """
    all_modules = fullname.split('.')
    parent_module_fullname = '.'.join(all_modules[:-1])
    if parent_module_fullname:
      __import__(parent_module_fullname)
      return sys.modules[parent_module_fullname]
    return None

  def _get_parent_search_path(self, fullname):
    """Determines the search path of a module's parent package.

    Args:
      fullname: Full name of the module to look up (e.g., foo.bar).

    Returns:
      Tuple (submodule, search_path) where:
        submodule: The last portion of the module name from fullname (e.g.,
          if fullname is foo.bar, then this is bar).
        search_path: List of paths that belong to the parent package's search
          path or None if there is no parent package.

    Raises:
      ImportError exception if the module or its parent could not be found.
    """
    _, _, submodule = fullname.rpartition('.')
    parent_package = self._get_parent_package(fullname)
    search_path = sys.path
    if parent_package is not None and hasattr(parent_package, '__path__'):
      search_path = parent_package.__path__
    return submodule, search_path

  def _get_module_info(self, fullname):
    """Determines the path on disk and the search path of a module or package.

    Args:
      fullname: Full name of the module to look up (e.g., foo.bar).

    Returns:
      Tuple (pathname, search_path, submodule, loader) where:
        pathname: String containing the full path of the module on disk,
            or None if the module wasn't loaded from disk (e.g. from a zipfile).
        search_path: List of paths that belong to the found package's search
            path or None if found module is not a package.
        submodule: The relative name of the submodule that's being imported.
        loader: A PEP 302 compatible path hook. If this is not None, then the
            other elements will be None.
    """
    submodule, search_path = self._get_parent_search_path(fullname)
    _, pathname, description, loader = self._find_module_or_loader(
        submodule, fullname, search_path)
    if loader:
      return None, None, None, loader
    else:
      _, _, file_type = description
      module_search_path = None
      if file_type == imp.PKG_DIRECTORY:
        module_search_path = [pathname]
        pathname = os.path.join(pathname, '__init__%spy' % os.extsep)
      return pathname, module_search_path, submodule, None

  def is_package(self, fullname):
    """Returns whether the module specified by fullname refers to a package.

    This implements part of the extensions to the PEP 302 importer protocol.

    Args:
      fullname: The fullname of the module.

    Returns:
      True if fullname refers to a package.
    """
    submodule, search_path = self._get_parent_search_path(fullname)
    _, _, description, loader = self._find_module_or_loader(
        submodule, fullname, search_path)
    if loader:
      return loader.is_package(fullname)
    _, _, file_type = description
    if file_type == imp.PKG_DIRECTORY:
      return True
    return False

  def get_source(self, fullname):
    """Returns the source for the module specified by fullname.

    This implements part of the extensions to the PEP 302 importer protocol.

    Args:
      fullname: The fullname of the module.

    Returns:
      The source for the module.
    """
    full_path, _, _, loader = self._get_module_info(fullname)
    if loader:
      return loader.get_source(fullname)
    if full_path is None:
      return None
    source_file = open(full_path)
    try:
      return source_file.read()
    finally:
      source_file.close()

  def get_code(self, fullname):
    """Returns the code object for the module specified by fullname.

    This implements part of the extensions to the PEP 302 importer protocol.

    Args:
      fullname: The fullname of the module.

    Returns:
      The code object associated the module.
    """
    full_path, _, _, loader = self._get_module_info(fullname)
    if loader:
      return loader.get_code(fullname)
    if full_path is None:
      return None
    source_file = open(full_path)
    try:
      source_code = source_file.read()
    finally:
      source_file.close()

    # Check that coding cookie is correct if present, error if not present and
    # we can't decode with the default of 'ascii'.  According to PEP 263 this
    # coding cookie line must be in the first or second line of the file.
    encoding = DEFAULT_ENCODING
    for line in source_code.split('\n', 2)[:2]:
      matches = CODING_MAGIC_COMMENT_RE.findall(line)
      if matches:
        encoding = matches[0].lower()
    # This may raise up to the user, which is what we want, however we ignore
    # the output because we don't want to return a unicode version of the code.
    source_code.decode(encoding)

    return compile(source_code, full_path, 'exec')


class PathOverrideImportHook(BaseImportHook):
  """An import hook that imports enabled modules from predetermined paths.

  Imports handled by this hook ignore the paths in sys.path, instead using paths
  discovered at initialization time.

  Attributes:
    extra_sys_paths: A list of paths that should be added to sys.path.
    extra_accessible_paths: A list of paths that should be accessible by
        sandboxed code.
  """

  def __init__(self, modules):
    self._modules = {}
    self.extra_accessible_paths = []
    self.extra_sys_paths = []
    for module in modules:
      module_path = self._get_module_path(module)
      if module_path:
        self._modules[module] = module_path
        if isinstance(module_path, str):
          package_dir = os.path.join(module_path, module)
          if os.path.isdir(package_dir):
            if module == 'PIL':
              self.extra_sys_paths.append(package_dir)
            else:
              self.extra_accessible_paths.append(package_dir)

  def find_module(self, fullname, unused_path=None):
    return fullname in self._modules and self or None

  def load_module(self, fullname):
    if fullname in sys.modules:
      return sys.modules[fullname]
    module_path = self._modules[fullname]
    if hasattr(module_path, 'load_module'):
      module = module_path.load_module(fullname)
    else:
      module = self._find_and_load_module(fullname, fullname, [module_path])
    module.__loader__ = self
    return module

  def _get_module_path(self, fullname):
    """Returns the directory containing the module or None if not found."""
    try:
      _, _, submodule = fullname.rpartition('.')
      f, filepath, _, loader = self._find_module_or_loader(
          submodule, fullname, sys.path)
    except ImportError:
      return None
    if f:
      f.close()
    if loader:
      return loader.find_module(fullname)
    return os.path.dirname(filepath)


class PyCryptoRandomImportHook(BaseImportHook):
  """An import hook that allows Crypto.Random.OSRNG.new() to work on posix.

  This changes PyCrypto to always use os.urandom() instead of reading from
  /dev/urandom.
  """

  def __init__(self, path):
    self._path = path

  @classmethod
  def find_module(cls, fullname, path=None):
    if fullname == 'Crypto.Random.OSRNG.posix':
      return cls(path)
    return None

  def load_module(self, fullname):
    if fullname in sys.modules:
      return sys.modules[fullname]
    __import__('Crypto.Random.OSRNG.fallback')
    module = self._find_and_load_module('posix', fullname, self._path)
    fallback = sys.modules['Crypto.Random.OSRNG.fallback']
    module.new = fallback.new
    module.__loader__ = self
    sys.modules[fullname] = module
    return module
