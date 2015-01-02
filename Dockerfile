FROM google/appengine-python27

# Install Python 3.4. We send over a script and run it, since we need to install
# it from source. Send it separately so that incremental changes to the SDK
# don't re-build and re-install Python.
ADD setup_deps.sh /home/vmagent/py3_setup/
RUN /home/vmagent/py3_setup/setup_deps.sh

RUN pip3 install CherryPy requests
ADD . /home/vmagent/python_vm_runtime_py3
ENTRYPOINT ["/home/vmagent/python_vm_runtime_py3/vmboot.py"]
