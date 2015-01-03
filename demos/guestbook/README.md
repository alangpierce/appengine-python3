# Getting started with the (Unofficial) Python 3 Runtime

Once you have your development environment set up for Managed VMs in general,
it's not hard to build and use the Python 3 Runtime. This guide shows how to
run the sample "guestbook" application from the
[App Engine Python tutorial](https://cloud.google.com/appengine/docs/python/gettingstartedpython27/introduction)
in Python 3. The details of the SDK are almost the same as for the official
Python 2.7 SDK, so see the official documentation for details on how to
use the datastore and other Google services from Python.

## 1. Set up your dependencies

Follow the steps in the
[Python Managed VMs setup guide](https://cloud.google.com/appengine/docs/python/managed-vms/)

In particular, you'll need to do the following:

* Create a Google Cloud Platform project where you'll deploy the code. (You can
  probably skip this step if you're just trying it out locally.)
* Install the `gcloud` tool and set your project ID to the project you created.
* Install Docker. On Mac, the easiest approach is to install `boot2docker`.

All of the commands below assume that your environment variables are set up for
Docker. For example, running `docker images` should not give an error message.
On Mac, you need to make sure you've run `$(boot2docker shellinit)` within your
current shell for docker commands to work.

## 2. Build the Python 3 Runtime Docker image

To build the SDK image, go to the top-level of this repo (the
`appengine-python3` directory, not the `guestbook` directory) and run

```
make build
```

This downloads a Debian base Docker image, installs Python 3.4, installs some
Python packages, sets some configuration settings, and loads the SDK code into
the image, then assigns the tag `alangpierce/appengine-python3` to the image
and saves it locally. See the `Makefile` and `Dockerfile` for details.

To make sure that it worked, you can run

```
docker images
```

And you should see an image with that tag.

## 3. Build and run the sample guestbook app Docker image

Then, in the `guestbook` directory (the same one as this README), run

```
make serve
```

This creates a Docker image starting from `alangpierce/appengine-python3`, then
loads the guestbook code to form a complete App Engine app. Again, see the
`Makefile` and `Dockerfile` for details; they're both very simple.

## 4. Deploy the Docker image to App Engine

First, note that projects are specified as a `gcloud` setting, not as an
`app.yaml` field like in the normal Python 2.7 SDK. You can double-check that
your project is configured correctly by running

```
gcloud config list project
```

If that looks, right, run the following form the `guestbook` directory to
deploy the app.

```
make deploy
```

This uploads the Docker image, so it may take some time.

## 5. Run the guestbook app with `dev_appserver.py`

First, make sure you have Python 3 installed, and install the runtime
dependencies (with `pip3 install -r requirements.txt`).

Then, change `app.yaml` to trick `dev_appserver` into thinking that it's
running the regular rumtime:

* Change `runtime: custom` to `runtime: python27`.
* Remove the `vm: true` line.

Then, from the top-level `appengine-python3` directory, run

```
./dev_appserver.py demos/guestbook
```

This should work just like in the the Python 2.7 SDK. Running the server in a
normal process is sometimes more convenient than running it inside a Docker
container (for example, it is easier to use a debugger).
