#!/usr/bin/env python

import datetime
import fcntl # Only avilablle on POSIX systems
import gzip
import json # Only available in Python 2.6+
import optparse
import os
import shutil
import sys
import tempfile
import time
import urllib
import urllib2


# How many bytes to buffer when retreiving data from server
CHUNK_SIZE = 4096
# How many seconds to wait before trying to fetch data again
RETRY_WAIT = 5


# Set up the command-line parameters
parser = optparse.OptionParser()
parser.add_option('-u', '--last-updated', dest='last_updated',
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
except ValueError, error:
	sys.exit('Failed to parse default_config.json with error: %s' % error)
# Update settings from the local configuration file
if options.local_config:
	try:
		config.update(json.load(open(options.local_config)))
	except ValueError, error:
		sys.exit('Failed to parse JSON data in %s with error: %s' % (
			options.local_config, error
		))
	except IOError, error:
		sys.exit('Failed to open config file % with error: %s' % (
			options.local_config, error
		))


class LastUpdateStore(object):
	"""
	This class is intended to be used only in conjunction with a "with"
	statement.
	"""
	
	def __init__(self, filename):
		self.filename = filename
		self.exit_functions = []
	
	def __enter__(self):
		"""
		This method is called when entering a "with" construct. It opens the
		store file for writing, creating it if necessary, and obtains an 
		exclusive lock on the file.
		
		Note that the type of lock provided by "fnctl" is non-enforsible, i.e.
		unless a process checks for its existence, and voluntarily obeys it, it
		will be able to write to the file. However, the main purpose of the lock
		is to prevent conflicts with other instances of pydbpintegrator, which
		will play nice.
		"""
		mode = 'r+' if os.path.exists(self.filename) else 'w'
		self.file = open(self.filename, mode)
		try:
			fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
		except IOError, e:
			sys.exit('Unable to obtain lock on %s, probably due to other ' 
				'running instances of pydbpintegrator.' % self.filename)
		return self
	
	def __exit__(self, *args, **kwargs):
		"""
		Called when leaving a "with" construct, releases lock, closes file and
		runs any queued on_exit functions.
		"""
		fcntl.flock(self.file, fcntl.LOCK_UN)
		self.file.close()
		for function in self.exit_functions:
			function()
	
	def on_exit(self, function):
		"""
		Registeres a provided function in a (FIFO) queue, to be executed when
		leaving the "with" construct.
		"""
		self.exit_functions += [function]
	
	def read(self):
		"""Returns the contents of the file."""
		self.file.seek(0)
		return self.file.read().strip()
	
	def write(self, value):
		"""Replaces contents of the file with provided value."""
		self.file.seek(0)
		self.file.truncate()
		self.file.write(unicode(value))
		self.file.flush()


class UpdateDate(object):
	
	def __init__(self, value=None):
		self.values = []
		if value is not None:
			self.set_value(value)
	
	def for_comparison(self):
		"""
		The lastPublishedFile.txt supplied by the server is actually always one
		larger than the actual last published file, so in comparison contexts
		we'll compare against an incremented version of self, unless it's not a
		fully formed date.
		"""
		if len(self.values) < 5:
			return unicode(self)
		else:
			return u'-'.join(self._string_values(increment=1))
	
	def __unicode__(self):
		"""
		Returns the date in YYYY-MM-DD-HH-IIIIII format, which corresponds to
		the format used in lastPublishedFile.txt on the DBPedia Live server.
		Incomplete dates will not be returned as fully formed.
		"""
		return u'-'.join(self._string_values())
	
	def _string_values(self, limit=5, increment=0):
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
				yield str(value + increment).zfill(6)
	
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


class SPARQL(object):
	def __init__(self, endpoint, graph=None, username=None, password=None):
		self.endpoint = endpoint
		self.graph = graph
		if username or password:
			password_manager = urllib2.HTTPPasswordMgrWithDefaultRealm()
			password_manager.add_password(None, endpoint, username, password)
			handler = urllib2.HTTPBasicAuthHandler(password_manager)
			self.opener = urllib2.build_opener(handler)
		else:
			self.opener = urllib2.build_opener()
		self.opener.addheaders.append((
			'Accept',
			'application/sparql-results+json,text/javascript,application/json',
		))
		
	
	def query(self, query):
		data = urllib.urlencode({
			'query': query,
			'default-graph-uri': self.graph,
			'format': 'JSON',
		})
		try:
			self.opener.open(self.endpoint, data)
		except urllib2.HTTPError, e:
			# e.g. Virtuoso 22007 Error DT006:
			# Cannot convert 2007-02-31 to datetime
			# TODO: log these
			pass
	
	def insert(self, triple):
		if triple:
			return self.query('INSERT { %s }' % triple)
	
	def delete(self, triple):
		if triple:
			return self.query('DELETE { %s } WHERE { %s }' % (
				triple, triple
			))


with LastUpdateStore(config['last_updated_store']) as last_update_store:
	# If the script was called to update the last updated value, update the
	# store and quit
	if options.last_updated:
		last_update_store.write(UpdateDate(options.last_updated))
		# TODO: Validation
		print 'Last updated date updated.'
		sys.exit()
	
	# Create a temporary directory for the downloaded files
	if not config['temp_directory']:
		temp_directory = tempfile.mkdtemp(prefix='pydbp')
	else:
		temp_directory = config['temp_directory']
		if not os.path.exists(temp_directory):
			os.makedirs(temp_directory)
		assert os.access(temp_directory, os.W_OK), \
			'Can\'t write to temp directory %s' % temp_directory
	if config['clear_temp_files']:
		last_update_store.on_exit(lambda: shutil.rmtree(temp_directory))
	
	# Set up the SPARQL "connection"
	sparql = SPARQL(
		config['sparql_endpoint'],
		config['sparql_default_graph'],
		config['sparql_username'],
		config['sparql_password'],
	)
	
	last_updated = UpdateDate(last_update_store.read())	
	last_published = None # Nothing is smaller than None
	while True:
		if last_updated.for_comparison() < last_published:
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
				assert last_updated.for_comparison() < last_published, \
					'Invalid last published date provided by server.'
				last_update_store.write(last_updated)
				continue
			# TODO: wrap in a try that catches KeyboardInterrupt
			if added_file:
				# Unzip and load added file
				print 'Unzipping and loading %s' % added_file
				for triple in gzip.open(added_file):
					sparql.insert(triple)
			if removed_file:
				# Unzip and load removed file
				print 'Unzipping and loading %s' % removed_file
				for triple in gzip.open(removed_file):
					sparql.delete(triple)
			last_update_store.write(last_updated)
		else:
			# If this isn't the first run through the loop, wait before checking
			# the server for updates
			if last_published is not None:
				time.sleep(RETRY_WAIT)
			# Get the last updated date from the server
			last_published = urllib2.urlopen(
				'%s/lastPublishedFile.txt' % config['live_server'].rstrip('/')
			).read().strip()

