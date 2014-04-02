# -*- coding: utf-8 -*-

from __future__ import print_function

import os
import logging
import traceback
import dictdiffer
import yaml
import json

import beanstalkc

import utils
import amendment_info
import vote_info
import bill_info


def slurp_json(path):
  """Reads entire JSON file, returns parsed content."""
  with open(path, 'r') as infile:
    return json.load(infile)


def slurp_yaml(path):
  """Reads entire YAML file, returns parsed content."""
  with open(path, 'r') as infile:
    return yaml.load(infile)


class Beanstalk(object):
    Connection = None
    Config = None

    @classmethod
    def tube_name(cls):
      return cls.Config['tube']

    @classmethod
    def validate_config(cls, path):
      config = slurp_yaml(path)
      assert 'beanstalk' in config
      assert 'connection' in config['beanstalk']
      assert 'host' in config['beanstalk']['connection']
      assert 'port' in config['beanstalk']['connection']
      assert 'tube' in config['beanstalk']
      assert config['beanstalk']['tube'] is not None
      assert config['beanstalk']['tube'] != ''
      cls.Config = config['beanstalk']

    @classmethod
    def connection_guard(cls, reconnect=False):
      if cls.Connection is None or reconnect == True:
        conn = beanstalkc.Connection(**cls.Config['connection'])
        assert conn is not None
        cls.Connection = conn
        cls.Connection.use(cls.tube_name())

    @classmethod
    def resilient_put(cls, msg):
      cls.connection_guard(reconnect=False)
      for _ in range(2):
        try:
          cls.Connection.put(json.dumps(msg))
          logging.warn(u"Queued {0} to beanstalkd queue '{1}'.".format(msg, cls.tube_name()))
          break
        except beanstalkc.SocketError:
          logging.warn(u"Lost connection to beanstalkd. Attempting to reconnect.")
          cls.connection_guard(reconnect=True)
        except Exception as e:
          logging.warn(u"Ignored exception while queueing message to beanstalkd: {0} {1}".format(unicode(type(e)), unicode(e)))
          traceback.print_exc()
          break


def data_actually_changed(obj, dest):
  """
  Returns a boolean value representing whether the differences between the
  given object 'obj' and the parsed contents of the 'dest' file are limited to
  the 'updated_at' key.
  """
  if os.path.exists(dest) == False:
    return True
  existing = slurp_json(dest)
  diff = list(dictdiffer.diff(existing, obj))
  if len(diff) == 1 and diff[0][:2] == (u'change', u'updated_at'):
    return False
  return True


def output_vote_patch(vote, options, id_type=None):
  """
  Abbreviated form of vote_info.output_vote that avoids writing xml files and
  avoids writing json files when the data has not changed.
  """
  destpath = vote_info.output_for_vote(vote["vote_id"], "json")
  if data_actually_changed(vote, destpath):
    utils.write(
      json.dumps(vote, sort_keys=True, indent=2, default=utils.format_datetime),
      destpath
    )
    Beanstalk.resilient_put({'roll_call_id': vote['vote_id']})


def output_bill_patch(bill, options):
  """
  Abbreviated form of bill_info.output_bill that avoids writing xml files and
  avoids writing json files when the data has not changed.
  """
  destpath = bill_info.output_for_bill(bill['bill_id'], "json")
  if data_actually_changed(bill, destpath):
    utils.write(
      json.dumps(bill, sort_keys=True, indent=2, default=utils.format_datetime),
      destpath
    )
    Beanstalk.resilient_put({'bill_id': bill['bill_id'], 'force': True})


def output_amendment_patch(amdt, options):
  """
  Abbreviated form of amendment_info.output_amendment that avoids writing xml
  files and avoids writing json files when the data has not changed.
  """
  destpath = amendment_info.output_for_amdt(amdt['amendment_id'], "json")
  if data_actually_changed(amdt, destpath):
    utils.write(
      json.dumps(amdt, sort_keys=True, indent=2, default=utils.format_datetime),
      destpath
    )
    Beanstalk.resilient_put({'amendment_id': amdt['amendment_id']})


def patch(task_name):
  """
  This function is called by the 'run' script. It is responsible for
  monkey-patching the appropriate methods.
  """
  vote_info.output_vote = output_vote_patch
  bill_info.output_bill = output_bill_patch
  amendment_info.output_amendment = output_amendment_patch


Beanstalk.validate_config('config.yml')
