#!/usr/bin/env python
"""
S3 Stat
=======

This python module uses the really nice `goaccess <http://goaccess.io/>`_ utility
to provide you with an amazing Amazon log file analyser tool that is relatively easy to install, and is extremely
easy to extend.
GOACCESS version needed: 0.8.5

Installation
-------------

::

    pip install s3stat

This installs `s3stat.py` in your PYTHONPATH in case you would like to run it from the command line.

Quickstart
------------

Install goaccess
..................

You should install `goaccess <http://goaccess.io/>`_

.. note::
    Cloudfront log file processing requires goaccess 0.7.1+

Generating an AWS user
........................

First you should create a user that has approriate rights to read your log files, and you should have its AWS access keys ready.

#. Log in to the `aws console <https://console.aws.amazon.com/iam/home?#users>`_
#. Create a new user and select the option to generate an access key for the user
#. Save the access key and secure keys, as these will be needed soon
#. Open the *Permissions* tab for the user, and attach a new user policy. Select custom policy, and copy the following::

        {
          "Statement": [
            {
              "Sid": "Stmt1334764540928",
              "Action": [
                "s3:GetBucketAcl",
                "s3:GetBucketLogging",
                "s3:GetObject",
                "s3:ListAllMyBuckets",
                "s3:ListBucket",
                "s3:PutBucketAcl",
                "s3:PutBucketLogging",
                "s3:PutObject",
                "s3:PutObjectAcl"
              ],
              "Effect": "Allow",
              "Resource": [
                "arn:aws:s3:::*"
              ]
            },
            {
              "Sid": "Stmt1334764631669",
              "Action": [
                "cloudfront:GetDistribution",
                "cloudfront:GetDistributionConfig",
                "cloudfront:GetStreamingDistribution",
                "cloudfront:GetStreamingDistributionConfig",
                "cloudfront:ListDistributions",
                "cloudfront:ListStreamingDistributions",
                "cloudfront:UpdateDistribution",
                "cloudfront:UpdateStreamingDistribution"
              ],
              "Effect": "Allow",
              "Resource": [
                "*"
              ]
            }
          ]
        }

Set up logging in your buckets
...............................

First you should ask Amazon to generate logs for your buckets and cloudfront distributions.

Run this script
................

::

    s3stat.py <aws key> <aws secret> <bucket> <log_path>

This will download all the log files for today, and start a goaccess instance in your console.

For further options you might run::

    s3stat.py -h


Extending
----------

Actually s3stat was designed to be easy to add to your pythonic workflow, as a result it defines
a single class that you can subclass to process the results in json format.::

    import s3stat

    class MyS3Stat(s3stat.S3Stat):

        def process_results(self, json):
            print json

        def process_error(self, exception, data=None):
            print data
            raise exception

    mytask = MyS3Stat(bucket, log_path, for_date, (aws_key, aws_secret))
    mytask.run()

Where the `aws_*` parameters are optional, if missing then they are taken from the environment variables as provided by boto.
The process_error method currently is called only when the JSON decoding fails, thus `data` is the non-decodeable string, while
exception is the ValueError raised by Python.

ToDo
-----

* provide a command that adds logging to specified buckets and cloudfront distributions

"""
import ssl
import threading
from boto.s3.connection import S3Connection
import subprocess
from datetime import datetime, date
import argparse
import tempfile
import json
import gzip
import logging
import Queue
from StringIO import StringIO

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class ConcatThread(threading.Thread):
    """
    This threads creates the concatenated log file

    Following http://stackoverflow.com/questions/11983938/python-appending-to-same-file-from-multiple-threds
    """

    def __init__(self, outqueue, outfile):
        threading.Thread.__init__(self)
        self.queue = outqueue
        self.outfile = outfile

    def run(self):
        while True:
            data = self.queue.get()
            self.outfile.write(data)
            self.queue.task_done()

class DownloadLogThread(threading.Thread):
    """
    This thread downloads the small log snippets, and passes their content towards
    the ConcatThread for further processing
    """

    def __init__(self, in_queue, out_queue, is_cloudfront):
        threading.Thread.__init__(self)
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.is_cloudfront = is_cloudfront

    def read_log(self, item):
        data = item.get_contents_as_string()
        if self.is_cloudfront:
            f = StringIO(data)
            data = gzip.GzipFile(fileobj=f, mode='rb').read()
            f.close()
        return data

    def run(self):
        while True:
            try:
                item = self.in_queue.get()
                data = self.read_log(item)
                self.out_queue.put(data)
                self.in_queue.task_done()
            except Queue.Empty:
                self.join()
            except ssl.SSLError:
                logger.error('Error while downloading the stats',
                             extra={
                                 'stack': True,
                                 })
                continue


class S3Stat(object):
    """
    We download the log files from S3, then concatenate them, and pass the results to goaccess. It gives back a JSON
    that we can handle further.
    """
    _num_threads = 10

    def __init__(self, input_bucket, input_prefix, date_filter, aws_keys=None, is_cloudfront=False):
        """
        :param input_bucket: the amazon bucket to download log files from
        :param input_prefix: only log files with the given prefix will be downloaded
        :param date_filter: only log files with prefix+date_filter will be downloaded
        :param aws_keys: a list of (aws key, secret key)
        :param is_cloudfront: set to True for Cloudfront format processing, defaults to S3 format
        """
        self.input_bucket = input_bucket
        self.date_filter = date_filter
        self.is_cloudfront = is_cloudfront
        self.input_prefix = input_prefix + date_filter.strftime("%Y-%m-%d")
        self.aws_keys = aws_keys

    def _create_goconfig(self):
        """
        Creates a temporary goaccessrc file with the necessary formatting
        """
        self.configfile = tempfile.NamedTemporaryFile()
        log_content = "color_scheme 0"
        if self.is_cloudfront:
            log_content += """
log_format CLOUDFRONT
"""
        else:
            log_content += """
log_format AWSS3
"""
        self.configfile.write(log_content)
        self.configfile.flush()

    def download_logs(self, outfile):
        """
        Downloads logs from S3 using Boto.
        """
        if self.aws_keys:
            conn = S3Connection(*self.aws_keys)
        else:
            conn = S3Connection()

        mybucket = conn.get_bucket(self.input_bucket)
        log_file_queue = Queue.Queue()
        log_string_queue = Queue.Queue()
        try:
            #spawn the thread for parallel downloads
            for i in range(0, self._num_threads):
                t = DownloadLogThread(log_file_queue, log_string_queue, self.is_cloudfront)
                t.setDaemon(True)
                t.start()
            t = ConcatThread(log_string_queue, outfile)
            t.setDaemon(True)
            t.start()

            for item in mybucket.list(prefix=self.input_prefix):
                log_file_queue.put(item)
            # wait until the queues are emptied
            log_file_queue.join()
            log_string_queue.join()
        finally:
            # finally we can clear our threads
            for t in threading.enumerate():
                del t
            logger.debug("Downloading of logs completed")

    def process_results(self, json_obj, error=None):
        """
        This is the main method to be overwritten by implementors.

        :param json: A JSON object result from goaccess to be processed further.
        """
        open('s3output.html', 'w').write(json_obj)
        # logger.debug(json.dumps(json_obj))

    def process_error(self, exc, data=None):
        """
        This is the error handling method to be overwritten by implementers.

        :param exc: the exception object raised and catched somewhere during processing
        :param data: an optional attribute that might help further processing
        :returns: the returned value will be returned from the main `run` method.
        """
        print data
        raise exc

    def run(self, format="json"):
        """
        This runs the whole machinery, and calls the process_results method if format was given.

        By default it runs the goaccess script, and shows the results in the terminal.
        In json format is requested, process_results is called with the corresponding JSON dict. Otherwise
        it's called with a simple string.

        :param format: String optional, one of json, html or csv
        """
        self._create_goconfig()
        with tempfile.NamedTemporaryFile() as tempLog:
            self.download_logs(tempLog)
            tempLog.flush()  # needed to have the temp file written for sure
            logger.debug("Creating report")
            command = ["goaccess", "-f", tempLog.name, "-p", self.configfile.name]
            if format:
                command += [ "-o", format]
            server = subprocess.Popen(command, stdout=subprocess.PIPE if format else None)
            out, err = server.communicate()
        if format:
            if format == "json":
                try:
                    out = json.loads(out)
                except ValueError as e:
                    return self.process_error(e, out)

            self.process_results(out)

        return True

# def enable_logging(args):
#     if args.aws_key and args.aws_secret:
#         conn = S3Connection(aws_key, aws_secret)
#     else:
#         conn = S3Connection()

#     mybucket = conn.get_bucket(args.input_bucket)
#     mybucket.enable_logging(target_bucket=args.output_bucket, target_prefix=args.output_prefix)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Downloads logs from S3, and parses them with goaccess.")

    parser.add_argument("aws_key", help="Amazon identification key", default=None)
    parser.add_argument("aws_secret", help="Amazon identification key secret", default=None)
    parser.add_argument("input_bucket", help="Input bucket where logs are stored")
    parser.add_argument("input_prefix", help="Path inside the input bucket where logs are stored")
    parser.add_argument("-c", "--cloudfront", help="Cloudfront log processing", action="store_true", default=False)
    # Add logging related subcommand
    # parser.add_argument("--output_bucket", help="Output bucket for logging")
    # parser.add_argument("--output_prefix", help="Output prefix for generating log files in output bucket.", default="s3stat/access_log-")
    parser.add_argument("-o", "--output", help="Output format. One of html, json or csv.", default=None)
    parser.add_argument("-v", "--verbose", help="Verbose output", action="store_true", default=False)
    parser.add_argument("-d", "--date", help="The date to run the report on in YYYY-MM-DD format. Defaults to today.")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig()
        logger.setLevel(logging.DEBUG)

    if args.date:
        given_date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        given_date = date.today()

    if args.aws_key and args.aws_secret:
        aws_keys = (args.aws_key, args.aws_secret)
    else:
        aws_keys = None

    processor = S3Stat(args.input_bucket, args.input_prefix, given_date, aws_keys, args.cloudfront)
    processor.run(args.output)
