#!/usr/bin/env python3

# UPDATE LOG:
# Originally designed for Python 2.
# Updated 20191024 with Python2/3 compatibility, further diagnostics
# Updated Oct 2020 by CSIDs for MP revision, h/cpp source upload as zip,
#     and for API v2; partIds injected by prerelease build step

# MAINTAINER NOTES:
# Make sure the assignment key (akey) and Part IDs (partIds) in this script
# are up to date for the current installation of the assignment. Look in the
# definition of the submit function.

# This message was primarily meant for MOOC students
def display_prereqs_notice():
  print('\nREQUIREMENTS: To work on this assignment, we assume you already know how to')
  print('program in C++, you have a working Bash shell environment set up (such as')
  print('Linux, or Windows 10 with WSL installed for Ubuntu Linux and a Bash shell,')
  print('or macOS with developer tools installed and possibly additional Homebrew')
  print('tools. If you are not clear yet what these things are, then you need to')
  print('take another introductory course before working on this assignment. The')
  print('University of Illinois offers some intro courses on Coursera that will help')
  print('you understand these things and set up your work environment.\n')

import hashlib
import random
import email
import email.message
import email.encoders
# import StringIO # unused
import sys
import subprocess
import json
import os
import os.path
import base64
from io import BytesIO
from zipfile import ZipFile

# Python2/3 compatibility hacks: ----------------------------

# The script looks for this file to make sure it's the right working directory
anchoring_file = 'Application.cpp'

# Message displayed if compatibility hacks fail
compat_fail_msg = '\n\nERROR: Python 3 compatibility fix failed.\nPlease try running the script with the "python2" command instead of "python" or "python3".\n\n'
wrong_dir_msg = '\n\nERROR: Please run this script from the same directory where ' + anchoring_file + ' is.\n\n'

if not os.path.isfile(anchoring_file):
  print(wrong_dir_msg)
  raise Exception(wrong_dir_msg)
else:
  pass

try:
  raw_input
except:
  # NameError
  try:
    raw_input = input
  except:
    raise Exception(compat_fail_msg)

# urllib2 hacks based on suggestions by Ed Schofield.
# Link: https://python-future.org/compatible_idioms.html?highlight=urllib2
try:
  # Python 2 versions
  from urlparse import urlparse
  from urllib import urlencode
  from urllib2 import urlopen, Request, HTTPError
except ImportError:
  # Python 3 versions
  try:
    from urllib.parse import urlparse, urlencode
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError
  except:
    raise Exception(compat_fail_msg)

# End of compatibility hacks ---------------------------

# Begin submission stuff -------------------------------

# ---
# The "### INJECTION_SITE" is a magic comment. Don't change it.
# The results should resemble, e.g.:
# injected_akey = "asdfqwertzxcv"
# injected_partIds = ['TyUiO', 'GhUjI', 'KjIuY', 'EfGhR']
# injected_partNames = ['A Test', 'B Test', 'C Test', 'D Test']
injected_akey = "Lm64BvbLEeWEJw5JS44kjw"
injected_partIds = ['PH3Q7', 'PIXym', 'mUKdC', 'peNB6']
injected_partNames = ['Create Test', 'Delete Test', 'Read Test', 'Update Test'] 
### INJECTION_SITE
# ---

def get_injected_akey():
  global injected_akey
  return injected_akey.strip()

def get_injected_partIds():
  global injected_partIds
  for i in range(len(injected_partIds)):
    injected_partIds[i] = injected_partIds[i].strip()
    assert "" != injected_partIds[i], "partId can't be blank"
  return injected_partIds

def get_injected_partNames():
  global injected_partNames
  for i in range(len(injected_partNames)):
    injected_partNames[i] = injected_partNames[i].strip()
    assert "" != injected_partNames[i], "part displayName can't be blank"
  return injected_partNames

def assert_injections():
  try:
    akey = get_injected_akey()
    assert "" != akey , "akey is empty string"
    partIds = get_injected_partIds()
    assert len(partIds) != 0 , "partIds has 0 length"
    partNames = get_injected_partNames()
    assert len(partNames) != 0 , "partNames has 0 length"
    assert len(partNames) == len(partIds) , "partIds and partNames have different lengths"
  except Exception as e:
    msg = "Couldn't read submission part ID data. Please contact the course staff about this."
    msg += " Original error: " + str(e)
    raise Exception(msg)
  return True

def submit():
  print('==\n== Submitting Solutions \n==')

  (login_email, token) = loginPrompt()
  if not login_email:
    print('!! Submission Cancelled')
    return

  assert_injections()
  akey = get_injected_akey()
  partIds = get_injected_partIds()
  partNames = get_injected_partNames()

  submissions = [read_dbglog(i) for i in range(4)]

  submitSolution(login_email, token, akey, submissions, partNames, partIds)

# =========================== LOGIN HELPERS - NO NEED TO CONFIGURE THIS =======================================

class NullDevice:
  def write(self, s):
    pass

def loginPrompt():
  """Prompt the user for login credentials. Returns a tuple (login, token)."""
  (login_email, token) = basicPrompt()
  return login_email, token

def basicPrompt():
  """Prompt the user for login credentials. Returns a tuple (login, token)."""
  print('Please enter the email address that you use to log in to Coursera.')
  login_email = raw_input('Login (Email address): ')
  print('To validate your submission, we need your submission token.\nThis is the single-use key you can generate on the Coursera instructions page for this assignment.\nThis is NOT your own Coursera account password!')
  token = raw_input('Submission token: ')
  return login_email, token

def submit_url():
  """Returns the submission url."""
  return "https://www.coursera.org/api/onDemandProgrammingScriptSubmissions.v1"

def submitSolution(email_address, token, akey, submissions, partNames, partIds):
  """Submits a solution to the server. Returns (result, string)."""

  num_submissions = len(submissions)
  if len(partNames) != num_submissions:
    raise Exception('Config error: need one part name per submission item')
  if len(partIds) != num_submissions:
    raise Exception('Config error: need one part ID per submission item')

  parts_dict = dict()
  i = 0
  for p_ in partIds:
    parts_dict[partIds[i]] = {"output": submissions[i]}
    i += 1

  values = {
    "assignmentKey": akey,
    "submitterEmail": email_address,
    "secret": token,
    "parts": parts_dict
  }
  url = submit_url()
  # (Compatibility update) Need to encode as utf-8 to get bytes for Python3:
  data = json.dumps(values).encode('utf-8')
  req = Request(url)
  req.add_header('Content-Type', 'application/json')
  req.add_header('Cache-Control', 'no-cache')
  response = urlopen(req, data)
  return

## Read a debug log
def read_dbglog(partIdx):
  # open the file, get all lines
  f = open("dbg.%d.log" % partIdx)
  src = f.read()
  f.close()
  #print src
  return src

def main():
  try:
    submit()
    print('\n\nSUBMISSION FINISHED!\nYou can check your grade on Coursera.\n\n');
  except HTTPError:
    print('ERROR:\nSubmission authorization failed. Please check that your submission token is valid.')
    print('You can generate a new submission token on the Coursera instructions page\nfor this assignment.')

if __name__ == "__main__":
  args = sys.argv[1:]
  if len(args) > 0 and "--validate-injection" == args[0]:
    assert_injections()
  else:
    display_prereqs_notice()
    main()