#!/usr/bin/env python

import datetime
import fcntl # Only avilablle on POSIX systems
import json # Only available in Python 2.6+
import optparse
import os
import sys
import tempfile
import time
import urllib2


# How many bytes to buffer when retreiving data from server
CHUNK_SIZE = 4096
# How many seconds to wait before trying to fetch data again
RETRY_WAIT = 5


# Set up the command-line parameters
parser = optparse.OptionParser()
parser.add_option('-d', '--last-updated', dest='last_updated',
	help='Set the last update date of the DBPedia instance in '
	'YYYY-MM-DD-HH-IIIIII format and quit (at least YYYY-MM required).',
)
parser.add_option('-c', '--local-config', dest='local_config',
	help='The path to a local configuration JSON file.',
)
options, args = parser.parse_args()


# Load settings from the default configuration file
try:
	config = json.load(open('default_config.json'))
except ValueError, e:
	sys.exit('Failed to parse default_config.json with error: %s' % e)
# Update settings from the local configuration file
if options.local_config:
	try:
		config.update(json.load(open(options.local_config)))
	except ValueError, e:
		sys.exit('Failed to parse JSON data in %s with error: %s' % (
			options.local_config, e),
		)


class LastUpdate(object):
	"""
	This class is intended to be used only in conjunction with a "with"
	statement.
	"""
	
	def __init__(self, filename):
		self.filename = filename
		self.values = []
	
	def __enter__(self):
		# When entering a "with" construct, open file and obtain lock.
		# Note that the type of lock provided by fcntl is non-enforsible,
		# i.e. it has no effect if the other process doesn't check for it.
		self.file = open(self.filename, 'r+')
		try:
			fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
		except IOError, e:
			sys.exit('Unable to obtain lock on %s' % self.filename)
		return self
	
	def __exit__(self, *args, **kwargs):
		# When leaving a "with" construct, release lock and close file
		fcntl.flock(self.file, fcntl.LOCK_UN)
		self.file.__exit__(*args, **kwargs)
	
	def __cmp__(self, other):
		return cmp(unicode(self), other)
	
	def __unicode__(self):
		"""
		Returns the date in YYYY-MM-DD-HH-IIIIII format, which corresponds to
		the format used in lastPublishedFile.txt on the DBPedia Live server.
		Incomplete dates will not be returned as fully formed.
		"""
		return '-'.join(self._string_values())
	
	def _string_values(self, limit=5):
		"""
		Generate the values as strings with appropriate zero-padding, suitable 
		for constructing URLs or paths with.
		"""
		for i, value in enumerate(self.values):
			if i >= limit:
				return
			if i == 0:
				yield str(value).zfill(4)
			if 0 < i < 4:
				yield str(value).zfill(2)
			if i == 4:
				yield str(value).zfill(6)
	
	def set_value(self, value):
		"""
		Set the date from a provided string in YYYY-MM-DD-HH-IIIIII format. At
		least YYYY-MM are required, and the components should be numeric, but
		not necessarily zero-padded.
		"""
		assert len(value.split('-')) > 1, 'A minimum of year and month are needed.'
		self.values = map(int, value.split('-'))
	
	def increment(self):
		assert 1 < len(self.values) < 6, 'Unexpected update date length.'
		if len(self.values) == 2:
			# Timedelta is of no help when incrementing months, so we have to 
			# do it "by hand"
			if self.values[1] > 11:
				self.values[0] += 1
				self.values[1] = 1
			else:
				self.values[1] += 1
			self.values.extend([0,0,0,])
		elif len(self.values) == 3:
			value_date = datetime.date(*self.values)
			value_date += datetime.timedelta(days=1)
			# The timetuple comes pre-padded with zeroes
			self.values = list(value_date.timetuple())[:5]
		elif len(self.values) == 4:
			value_datetime = datetime.datetime(*self.values)
			value_datetime += datetime.timedelta(hours=1)
			self.values = list(value_datetime.timetuple())[:5]
		else:
			self.values[4] += 1
	
	def finish_hour(self):
		"""
		Truncates the values, removing the final "item #" component. This
		ensures that a subsequent increment() call will proceed to the next 
		hour.
		"""
		assert len(self.values) >= 4, 'A fully formed update date is needed.'
		self.values = self.values[:4]
	
	def url(self):
		return '/'.join(self._string_values())
	
	def path(self):
		return os.path.join(*self._string_values(limit=4))
	
	def read(self):
		self.file.seek(0)
		self.set_value(self.file.read().strip())
	
	def write(self):
		self.file.seek(0)
		self.file.truncate()
		self.file.write(unicode(self))
		self.file.flush()


def urlretrieve(url, directory):
	"""
	Retrieve the file at the provided URL, and save it into the provided
	directory. On network or server errors other than a 404, keep trying to
	fetch the remore file, waiting RETRY_WAIT seconds between each attempt.
	
	Returns path to saved file on success, None if file not found.
	"""
	# Get filename from URL and construct full destination path
	_, filename = url.rsplit('/', 1)
	path = os.path.join(directory, filename)
	while True:
		# On non-404 errors, keep tring to fetch the file
		try:
			remote_file = urllib2.urlopen(url)
			with open(path, 'w') as destination:
				while True:
					# Get the data in chunks and save it locally
					buffer = remote_file.read(CHUNK_SIZE)
					if buffer:
						destination.write(buffer)
					else:
						# EOF has been reached
						return path
		except urllib2.URLError, error:
			if getattr(error, 'code', None) == 404:
				return None
			print 'Failed to fetch %s, retrying...' % url
			# TODO: count and log failures
		time.sleep(RETRY_WAIT)
	

with LastUpdate(config['last_updated_store']) as last_updated:
	# If the script was called to update the last updated value, update the
	# store and quit
	if options.last_updated:
		last_updated.set_value(options.last_updated)
		last_updated.write()
		# TODO: Validation
		print 'Last updated date updated.'
		sys.exit()
	else:
		last_updated.read()
	
	if not config['temp_directory']:
		temp_directory = tempfile.mkdtemp(prefix='pydbp')
	else:
		temp_directory = config['temp_directory']
		if not os.path.exists(temp_directory):
			os.makedirs(temp_directory)
		assert os.access(temp_directory, os.W_OK), \
			'Can\'t write to temp directory %s' % temp_directory
	
	last_published = None # Nothing is smaller than None
	while True:
		if last_updated < last_published:
			# Incrementing also guarantees that we now have a fully formed
			# update date
			last_updated.increment()
			added_url = '%s/%s.added.nt.gz' % (
				config['live_server'].rstrip('/'), last_updated.url())
			removed_url = '%s/%s.removed.nt.gz' % (
				config['live_server'].rstrip('/'), last_updated.url())
			# Construct the directory to download the files into, creating it 
			# if necessary
			dl_directory = os.path.join(temp_directory, last_updated.path())
			if not os.path.exists(dl_directory):
				os.makedirs(dl_directory)
			# Download the files
			added_file = urlretrieve(added_url, dl_directory)
			removed_file = urlretrieve(removed_url, dl_directory)
			# If neither the added nor the removed file were on the server, we
			# assume that we've reached the end of files for the hour
			if added_file is None and removed_file is None:
				last_updated.finish_hour()
				# Sanity check
				assert last_updated < last_published, \
					'Invalid last published date provided by server.'
				last_updated.write()
				continue
			
			if added_file:
				# Unzip and load added file
				print 'Unzipping and loading %s' % added_file
			if removed_file:
				# Unzip and load removed file
				print 'Unzipping and loading %s' % removed_file
			last_updated.write()
		else:
			# If this isn't the first run through the loop, wait before checking
			# the server for updates
			if last_published is not None:
				time.sleep(RETRY_WAIT)
			# Get the last updated date from the server
			last_published = urllib2.urlopen(
				'%s/lastPublishedFile.txt' % config['live_server'].rstrip('/')
			).read().strip()
