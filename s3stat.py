#!/usr/bin/env python
"""
S3 Stat
=======

This python module uses the really nice `goaccess <http://goaccess.prosoftcorp.com/>`_ utility
to provide you with an amazing Amazon log file analyser tool that is relatively easy to install, and is extremely
easy to extend.

Installation
-------------

::
    
    pip install s3stat

This installs `s3stat.py` in your PYTHONPATH in case you would like to run it from the command line.

Quickstart
------------

Install goaccess
.................. 

You should install `goaccess <http://goaccess.prosoftcorp.com/>`_

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

        def process(self, json):
            print json

    mytask = MyS3Stat(bukcet, log_path, for_date, (aws_key, aws_secret))
    mytask.run()

Where the `aws_*` parameters are optional, if missing then they are taken from the environment variables as provided by boto.

ToDo
-----

* provide a command that adds logging to specified buckets and cloudfront distributions

"""
import os
from boto.s3.connection import S3Connection
import subprocess
from datetime import datetime, date
import argparse
import tempfile
import json
import logging

logger = logging.getLogger(__name__)

class S3Stat(object):
    """
    We download the log files from S3, then concatenate them, and pass the results to goaccess. It gives back a JSON 
    that we can handle further.
    """

    def __init__(self, input_bucket, input_path, date_filter, aws_keys=None):
        """
        :param aws_keys: a list of (aws key, secret key)
        """
        self.input_bucket = input_bucket
        self.input_path = input_path
        self.date_filter = date_filter
        self.aws_keys = aws_keys

    def _create_goconfig(self):
        """
        Creates a temporary goaccessrc file with the necessary formatting
        """
        self.configfile = tempfile.NamedTemporaryFile()
        self.configfile.write("""color_scheme 0
date_format %d/%b/%Y
log_format %^ %^ [%d:%^] %h %^ %^ %^ %^ "%^ %r %^" %s %^ %b %^ %^ %^ "%^" "%u" %^
""")
        self.configfile.flush()

    def is_needed(self, filename):
        """
        Only files that return true will be processed. 

        By default the file name should start with `access_log` and should contain the date filtered.
        """
        return "access_log-" in filename and self.date_filter.strftime("%Y-%m-%d") in filename

    def concat_files(self, outfile, filename):
        with open(filename) as infile:
            outfile.write(infile.read())

    def download_logs(self):
        """
        Downloads logs from S3 using Boto.
        """
        if self.aws_keys:
            conn = S3Connection(*self.aws_keys)
        else:
            conn = S3Connection()

        mybucket = conn.get_bucket(self.input_bucket)

        tempdir = tempfile.mkdtemp()
        for item in mybucket.list(prefix=self.input_path):
            if self.is_needed(item.key):
                local_file = os.path.join(tempdir, item.key.split("/")[-1])
                logger.debug("Downloading %s to %s" % (item.key, local_file))
                item.get_contents_to_filename(local_file)
                yield local_file

    def process_results(self, json):
        """
        This is the main method to be overwritten by implementors.

        :param json: A JSON object result from goaccess to be processed further.
        """
        logger.debug(json)

    def run(self, format="json"):
        """
        This runs the whole machinery, and calls the process_results method if format was given. 

        By default it runs the goaccess script, and shows the results in the terminal. 
        In json format is requested, process_results is called with the corresponding JSON dict. Otherwise
        it's called with a simple string.

        :param format: String optional, one of json, html or csv
        """
        self._create_goconfig()
        logs = self.download_logs()
        with tempfile.NamedTemporaryFile() as tempLog:
            for downloaded in logs:
                self.concat_files(tempLog, downloaded)

            tempLog.flush()  # needed to have the temp file written for sure
            logger.debug("Creating report")
            command = ["goaccess", "-f", tempLog.name, "-p", self.configfile.name]
            if format:
                command += [ "-o", format]
            server = subprocess.Popen(command, stdout=subprocess.PIPE if format else None)
            out, err = server.communicate()

        if format:
            if format == "json":
                out = json.loads(out)
            self.process_results(out)
        return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Downloads logs from S3, and parses them with goaccess.")

    parser.add_argument("aws_key", help="Amazon identification key", default=None)
    parser.add_argument("aws_secret", help="Amazon identification key secret", default=None)
    parser.add_argument("input_bucket", help="Input s3 path where the logs are to be found (s3://[BUCKET]/[PATH]/)")
    parser.add_argument("input_path", help="Input s3 path where the logs are to be found (s3://[BUCKET]/[PATH]/)")
    parser.add_argument("-o", "--output", help="Output format. One of html, json or csv.", default=None)
    parser.add_argument("-v", "--verbose", help="Verbose output", action="store_true", default=False)
    parser.add_argument("-d", "--date", help="The date to run the report on in YYYY-MM-DD format")

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    if args.date:
        given_date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        given_date = date.today()

    if args.aws_key and args.aws_secret:
        aws_keys = (args.aws_key, args.aws_secret)
    else:
        aws_keys = None

    processor = S3Stat(args.input_bucket, args.input_path, given_date, aws_keys)
    processor.run(args.output)