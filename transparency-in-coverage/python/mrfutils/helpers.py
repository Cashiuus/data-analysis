import csv
import gzip
import hashlib
import json
import logging
import os
from io import BytesIO
from itertools import chain
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen
from zipfile import ZipFile

import requests

from exceptions import InvalidMRF

DOWNLOADS_DIR = Path('/processing/temp_downloads').resolve(strict=True)


log = logging.getLogger('mrfutils.helpers')
log.setLevel(logging.DEBUG)


def prepend(value, iterator):
	"""Prepend a single value in front of an iterator
	>>>  prepend(1, [2, 3, 4])
	>>>  1 2 3 4
	"""
	return chain([value], iterator)


def peek(iterator):
	"""
	Usage:
	>>> next_, iter = peek(iter)
	allows you to peek at the next value of the iterator
	"""
	try: next_ = next(iterator)
	except StopIteration: return None, iterator
	return next_, prepend(next_, iterator)


class JSONOpen:
	"""
	Context manager for opening JSON(.gz) MRFs.
	Usage:
	>>> with JSONOpen('localfile.json') as f:
	or
	>>> with JSONOpen(some_json_url) as f:
	including both zipped and unzipped files.
	"""

	def __init__(self, filename):
		self.filename = filename
		self.f = None
		self.r = None
		self.is_remote = None
		self.unzipped_filename = None

		parsed_url = urlparse(self.filename)
		self.suffix = ''.join(Path(parsed_url.path).suffixes)

		if not (
			self.suffix.lower().endswith('.json.gz') or
			self.suffix.lower().endswith('.json') or
			self.suffix.lower().endswith('.zip')
		):
			raise InvalidMRF(f'Suffix not JSON or ZIP: {self.filename=} {self.suffix=}')

		self.is_remote = parsed_url.scheme in ('http', 'https')

	def __enter__(self):

		if self.suffix.lower() == ".zip":
			self.download_zip_file(self.filename)
			self.is_remote = False
			self.suffix = Path(self.unzipped_filename).suffix
			log.debug(f"Zip downloaded, new suffix of file is: {self.suffix}")

		if (
			self.is_remote
			# endswith is used to protect against the case
			# where the filename contains lots of dots
			# insurer.stuff.json.gz
			and self.suffix.endswith('.json.gz')
		):
			self.s = requests.Session()
			self.r = self.s.get(self.filename, stream=True)
			self.f = gzip.GzipFile(fileobj=self.r.raw)

		elif (
			self.is_remote
			and self.suffix.endswith('.json')
		):
			self.s = requests.Session()
			self.r = self.s.get(self.filename, stream=True)
			self.r.raw.decode_content = True
			self.f = self.r.raw

		elif self.suffix == '.json.gz':
			if self.unzipped_filename:
				self.f = gzip.open(self.unzipped_filename, 'rb')
			else:
				self.f = gzip.open(self.filename, 'rb')
		else:
			if self.unzipped_filename:
				self.f = open(self.unzipped_filename, 'rb')
				log.info(f'Opened file: {self.unzipped_filename}')
			else:
				self.f = open(self.filename, 'rb')
				log.info(f'Opened file: {self.filename}')

		return self.f

	def __exit__(self, exc_type, exc_val, exc_tb):
		if self.is_remote:
			self.s.close()
			self.r.close()

		self.f.close()

		# Delete it so our HDD doesn't fill up
		if self.unzipped_filename:
			os.remove(self.unzipped_filename)
	
	def download_zip_file(self, url, destination=None):
		""" Download and extract a zip file. """
		if not destination:
			destination = DOWNLOADS_DIR
		
		if not os.path.isdir(destination):
			os.makedirs(destination)

		log.debug(f"Saving zip extracted contents to dir: {destination}")
		with urlopen(url) as zipres:
			with ZipFile(BytesIO(zipres.read())) as zfile:
				# ex: zipfile filelist: [<ZipInfo filename='2023-01-08_Health-Plans-Inc-(HPI)_index.json' compress_type=deflate filemode='-rwxrwxrwx' file_size=7347 compress_size=1044>]
				log.debug(f"zipfile filelist: {zfile.filelist}")
		
				files_in_zip = zfile.namelist()
				# ex: zipfile namelist: ['2023-01-08_Health-Plans-Inc-(HPI)_index.json']

				fname = zfile.extractall(destination)

		files_list = [os.path.join(destination, x) for x in files_in_zip]
		log.debug(f"Fetched zip and its extracted files are being returned: {files_list}")
		
		if fname is not None:
			self.unzipped_filename = os.path.join(destination, fname)
			log.debug(f"Fetched zip and its extracted filename is: {self.unzipped_filename}")
		
		self.unzipped_filename = files_list[0]
		return self.unzipped_filename



def import_csv_to_set(filename: str):
	"""Imports data as tuples from a given file."""
	items = set()

	with open(filename, 'r') as f:
		reader = csv.reader(f)
		for row in reader:
			row = [col.strip() for col in row]
			if len(row) > 1:
				items.add(tuple(row))
			else:
				item = row.pop()
				items.add(item)
		return items


def make_dir(out_dir):

	if not os.path.exists(out_dir):
		os.mkdir(out_dir)


def dicthasher(data: dict, n_bytes = 8) -> int:

	if not data:
		raise Exception("Hashed dictionary can't be empty")

	data = json.dumps(data, sort_keys=True).encode('utf-8')
	hash_s = hashlib.sha256(data).digest()[:n_bytes]
	hash_i = int.from_bytes(hash_s, 'little')

	return hash_i


def append_hash(item: dict, name: str) -> dict:

	hash_ = dicthasher(item)
	item[name] = hash_

	return item


def filename_hasher(filename: str) -> int:

	# retrieve/only/this_part_of_the_file.json(.gz)
	filename = Path(filename).stem.split('.')[0]
	file_row = {'filename': filename}
	filename_hash = dicthasher(file_row)

	return filename_hash

