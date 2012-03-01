#!/usr/bin/env python
#-*- coding: utf-8 -*-
from argparse import ArgumentParser
from sys import exit
from time import sleep
import httplib
import libxml2
import os
import pickle
import requests
import shelve
import urllib2

NAMESPACES = {
	"ead":"urn:isbn:1-931666-22-9",
	"xlink":"http://www.w3.org/1999/xlink",
	"rdf":"http://www.w3.org/1999/02/22-rdf-syntax-ns#",
	"madsrdf":"http://www.loc.gov/mads/rdf/v1#",
	"opensearch":"http://a9.com/-/spec/opensearch/1.1/",
	"cluster":"http://viaf.org/viaf/terms#",
	"xq":"http://www.loc.gov/zing/cql/xcql/",
	"srw":"http://www.loc.gov/zing/srw/"
}

ID_SUBJECT_RESOLVER = "http://id.loc.gov/vocabulary/subject/label/"
VIAF_SEARCH = "http://viaf.org/viaf/search"
RSS_XML = "application/rss+xml" 
APPLICATION_XML = "application/xml"
SHELF_FILE = "cache.db"

#===============================================================================
# HeadingNotFoundException
#===============================================================================
class HeadingNotFoundException(Exception):
	def __init__(self, msg, heading, type):
		super(HeadingNotFoundException, self).__init__(msg)
		"""
		@param msg: Message for logging
		@param heading: The heading we were searching when this was raised
		@param type: The type of heading (personal or corporate)  
		"""
		self.heading = heading
		self.type = type

#===============================================================================
# MultipleMatchesException
#===============================================================================
class MultipleMatchesException(Exception):
	def __init__(self, msg, heading, type, items):
		super(MultipleMatchesException, self).__init__(msg)
		"""
		@param msg: Message for logging
		@param heading: The heading we were searching when this was raised
		@param type: The type of heading (personal or corporate)  
		@param items: A list of 2-tuple (uri, label) possibilities
		"""
		self.heading = heading
		self.type = type
		self.items = items

#===============================================================================
# UnexpectedResponseException
#===============================================================================
# we throw when we get an enexpected (unhandled) HTTP response
class UnexpectedResponseException(Exception): pass

#===============================================================================
# Heading
#===============================================================================
class Heading(object):
	CORPORATE = "corporate"
	PERSONAL = "personal"
	SUBJECT = "subject"
	def __init__(self):
		self.value = ""
		"""Heading label (string) normalized from the source data"""
		self.type = ""
		"""'corporate', 'personal', or 'subject'"""
		self.found = ""
		"""boolean, True if one or more URIs was found"""
		self.alternatives = ""
		""""A list of 2-tuple (uri, label) possibilities"""
	@staticmethod	
	def pers_or_corp_from_node(node):
		# TODO: make sure that node.get_name() returns the local name, and not, 
		# e.g. ead:corpname when there is a namespace prefix
		if node.get_name() == "corpname": return Heading.CORPORATE
		else: return Heading.PERSONAL

#===============================================================================
# XPaths
#===============================================================================
class XPaths(object):
	"""
	Constants for getting at the relevant parts of the EAD doc.
	"""
	NAMES = "/ead:ead/ead:archdesc/ead:controlaccess/ead:corpname" + \
				"[not(@authfilenumber)]|" + \
		"/ead:ead/ead:archdesc/ead:controlaccess/ead:famname" + \
			"[not(@authfilenumber)]|" + \
		"/ead:ead/ead:archdesc/ead:controlaccess/ead:persname" + \
			"[not(@authfilenumber)]|" + \
		"/ead:ead/ead:archdesc/ead:did/ead:origination/*" + \
			"[not(@authfilenumber)]"
			
	NAMES_RECURSIVE = "//ead:corpname[not(@authfilenumber)]|" + \
					"//ead:famname[not(@authfilenumber)]|" + \
					"//ead:persname[not(@authfilenumber)]|" + \
					"//ead:origination/*[not(@authfilenumber)]"
				
	SUBJECTS = "//ead:archdesc/ead:controlaccess/ead:subject" + \
					"[not(@source = 'local') and not(@authfilenumber)]"
	
	SUBJECTS_RECURSIVE = "//ead:subject" + \
							"[not(@source = 'local') and not(@authfilenumber)]"

#===============================================================================
# _normalize_heading
#===============================================================================
def _normalize_heading(heading):

	"""
	@param heading: A heading from the source data.
	@return: A normalized version of the heading.
	 
	@note: 	Other users may need to modify or extend this function. This
	version, in order:
	 1. collapeses whitespace
	 2. strips spaces that trail or follow hyphens ("-")
	 3. strips trailing stops (".")
	"""
	collapsed = " ".join(heading.split()).replace(" -", "-").replace("- ", "-")
	if collapsed.endswith("."):
		stripped = collapsed[:-1]
	else:
		stripped = collapsed	 
	return stripped

#===============================================================================
# query_viaf
#===============================================================================
def query_viaf(name, type, accept=RSS_XML):
	"""
	@param name: name to look for in VIAF 
	@param type: Heading.PERSONAL or Heading.CORPORATE
	@param accept: MIME type for accept header
	@return: A 2-tuple (uri, label)
	
	@raise MultipleMatchesException: when multiple hits are found. The
	"items" property of the exception instance will contain a list of 
	(uri, label) 2-tuples that can be logged or commented into the data for
	human review.
	
	@raise HeadingNotFoundException: when no headings are found.
	
	@raise Exception: when something else goes wrong. Prefer to handle these
	at a higher level.
	"""
	q = 'local.' + type + 'Names+%3D+"' + name + '"+and+local.sources+any+"lc"'
	headers = {'Accept': accept}
	params = {"query":q}
	resp = requests.get(VIAF_SEARCH, headers=headers, params=params)
	ctxt = None
	doc = None
	try:
		doc = libxml2.parseDoc(resp.text.encode("UTF-8", errors="ignore"))
		
		ctxt = doc.xpathNewContext()
		for ns in NAMESPACES.keys():
			ctxt.xpathRegisterNs(ns, NAMESPACES[ns])

		count = int(ctxt.xpathEval("//opensearch:totalResults")[0].content)
		
		if count == 1:
			uri = ctxt.xpathEval("//link[parent::item]")[0].content
			label = ctxt.xpathEval("//title[parent::item]")[0].content
			return (uri, label)
		elif count == 0:
			msg = "Not found: " + name + os.linesep
			raise HeadingNotFoundException(msg, name, type)	
		elif count > 1:
			# check for an exact match, we'll return that
			if int(ctxt.xpathEval("count(//title[. = '" + name + "'])")) == 1:
				# (re. above magic: if count of titles with exactly our name is 1) 
				label = ctxt.xpathEval("//title[. = '" + name + "']")[0].content
				uri = ctxt.xpathEval("//item[title[. = '" + name + "']]/link")[0].content
				return (uri, label)
			else:
				# We make a list of (uri, authform ) two-tuples that the 
				# exception can report.
				msg = "Multiple matches for " + name + "\n"
				items = []
				for item in ctxt.xpathEval("//item"):
					uri = ""
					authform = ""
					for child in item.children:
						if child.type == "element":
							if child.name == "title":
								authform = child.content 
							elif child.name == "link":
								uri = child.content
							else: pass
					items.append((uri, authform))
				raise MultipleMatchesException(msg, name, type, items)
		else:
			raise Exception("Could not retrieve count (" + name + ")")
	
 	finally:
		# clean up!
		if ctxt != None: ctxt.xpathFreeContext()
		if doc != None: doc.freeDoc()
		
#===============================================================================
# query_lc
#===============================================================================
def query_lc(subject):
	"""
	@param subject: a name or subject heading
	@type subject: string
	
	@raise HeadingNotFoundException: when the heading isn't found
	
	@raise UnexpectedResponseException: when the initial response from LC is not 
		a 302 or 404 (404 should raise a HeadingNotFoundException)
	
	"""
	to_get = ID_SUBJECT_RESOLVER + subject
	headers = {"Accept":"application/xml"}
	resp = requests.get(to_get, headers=headers, allow_redirects=True)
	if resp.status_code == 200:
		uri = resp.headers["x-uri"]
		label = resp.headers["x-preflabel"]
		return uri, label
	elif resp.status_code == 404:
		msg = "Not found: " + subject + os.linesep
		raise HeadingNotFoundException(msg, subject, Heading.SUBJECT)
	else: # resp.status_code != 404 and status != 200:
		msg = " Response for \"" + subject + "\" was "
		msg += resp.status_code + os.linesep
		raise UnexpectedResponseException(msg)

#===============================================================================
# update_headings
#===============================================================================
def _update_headings(xpath, ctxt, shelf, annotate=False, verbose=False):
	for node in ctxt.xpathEval(xpath):
		try:
			heading = _normalize_heading(node.content)
			
			element_name = node.get_name()
			heading_type = ""
			if element_name == Heading.SUBJECT: heading_type = Heading.SUBJECT
			elif element_name == "corpname":  heading_type = Heading.CORPORATE
			else: heading_type == Heading.PERSONAL
				
			# Check the shelf right off
			if heading in shelf:
				cached = shelf[heading]
				if len(cached.alternatives) == 1:
					# we only get here if no exceptions above 
					if verbose:	os.sys.stdout.write("[Cache] Found: " + heading + "\n") 
					uri = cached.alternatives[0][0]
					node.setProp("authfilenumber", uri)
				elif len(cached.alternatives) > 1:
					msg = "[Cache] Multiple matches for " + heading + "\n"
					raise MultipleMatchesException(msg, heading, heading_type, cached.alternatives)
				else: # 0 
					msg = "[Cache] Not found: " + heading + "\n"
					raise HeadingNotFoundException(msg, heading, heading_type)
			else:
				if heading_type == Heading.SUBJECT:
					uri, auth = query_lc(heading)
					# we only get here if no exceptions above 
					if verbose:	os.sys.stdout.write("Found: " + heading + "\n")
					node.setProp("authfilenumber", uri)
					
				else:
					uri, auth = query_viaf(heading, Heading.pers_or_corp_from_node(node))
					# we only get here if no exceptions above 
					if verbose:	os.sys.stdout.write("Found: " + heading + "\n")
					node.setProp("authfilenumber", uri)

				# we put the heading we found in the db
				record = Heading()
				record.value = heading
				record.type = type
				record.found = True
				record.alternatives = [(uri, auth)]
				shelf[heading] = record

				node.setProp("authfilenumber", uri)
				
				sleep(1) # A courtesy to the services.

		except UnexpectedResponseException, e:
			os.sys.stderr.write(str(e))
		
		except HeadingNotFoundException, e:
			if verbose:
				os.sys.stderr.write(str(e))
			if not heading in shelf:
				# We still want to put this in the db
				record = Heading()
				record.value = e.heading
				record.type = e.type
				record.found = False
				record.alternatives = []
				shelf[heading] = record
		
		except MultipleMatchesException, m:
			if verbose:
				os.sys.stderr.write(str(m)) 
			if annotate:
				content = os.linesep + "Possible URIs:" + os.linesep
				for alt in m.items:
					content += alt[0] + " : " + alt[1] + os.linesep 
				comment = libxml2.newComment(content)
				node.addNextSibling(comment)
			if not heading in shelf:
				# We still want to put this in the db
				record = Heading()
				record.value = m.heading
				record.type = m.type
				record.found = True
				record.alternatives = m.items
				shelf[heading] = record

		except LookupError, e:
			record = Heading()
			record.value = heading
			record.type = heading_type
			record.found = True
			record.alternatives = []
			shelf[heading] = record
			e.message = "Error: " + e.message + " This is realted to VIAF sending data " + \
			" for \"" + heading + "\" that we can't parse. \nThis has been " + \
			"been noted in the cache and this heading will ignored in the " + \
			"future. Run again.\n" 
			raise e
		
class CLI(object):
	EX_OK = 0
	"""All good"""

	EX_SOMETHING_ELSE = 9 
	"""Something unanticipated went wrong"""
		
	EX_WRONG_USAGE = 64
	"""The command was used incorrectly, e.g., with the wrong number of 
	arguments, a bad flag, a bad syntax in a parameter, or whatever.""" 

	EX_DATA_ERR = 65
	"""The input data was incorrect in some way."""
		
	EX_NO_INPUT = 66
	"""Input file (not a system file) did not exist or was not readable."""
	
	EX_SOFTWARE = 70
	"""An internal software (not OS) error has been detected."""
	
	EX_CANT_CREATE = 73
	"""User specified output file cannot be created."""
	
	EX_IOERR = 74
	"""An error occurred while doing I/O on some file."""
		
	def __init__(self):
		
		# start by assuming something will go wrong:
		status = CLI.EX_SOMETHING_ELSE
		
		desc = "Adds id.loc.gov URIs to subject headings and VIAF URIs to " + \
				"name headings when established forms can be found."
		
		# TODO:
		epi = "TODO. See EX_* constants in CLI class for now."
	
		oHelp = "Path to the output file. Writes to stdout if no option " + \
			"is supplied."
		
		rHelp = "Recurse through the dsc. By default only the archdesc " + \
			"is treated."
		
		nHelp = "Try to find URIs for names."
		
		sHelp = "Try to find URIs for subjects."
	
		aHelp = "Annotate the record. When multiple matches are found XML " + \
			"comments containing the matches and their URIs will be added " + \
			"to the record."
			
		vHelp = "Print messages to stdout (one-hit headings) and stderr " + \
			"(zero or more than one hit headings)."

		parser = ArgumentParser(description=desc, epilog=epi)
		parser.add_argument("-o", "--output", default=None, required=False, dest="outpath", help=oHelp)
		parser.add_argument("-r", "--recursive", default=False, required=False, dest="recursive", action="store_true", help=rHelp)
		parser.add_argument("-n", "--names", default=False, required=False, dest="names", action="store_true", help=nHelp)
		parser.add_argument("-s", "--subjects", default=False, required=False, dest="subjects", action="store_true", help=sHelp)
		parser.add_argument("-a", "--annotate", default=False, required=False, dest="annotate", action="store_true", help=aHelp)
		parser.add_argument("-v", "--verbose", default=False, required=False, dest="verbose", action="store_true", help=vHelp)
		parser.add_argument("record", default=None)
		args = parser.parse_args()

		#=======================================================================
		# Checks on our args and options. We can exit before we do any work.
		#=======================================================================
		if not os.path.exists(args.record):
			os.sys.stderr.write("File " + args.record + " does not exist\n")
			exit(CLI.EX_NO_INPUT)
			
		if args.record == None:
			os.sys.stderr.write("No input file supplied. See --help for usage\n")
			exit(CLI.EX_WRONG_USAGE)
	
		if not args.names and not args.subjects:
			msg = "Supply -n and or -s to link headings. Use --help " + \
			"for more details.\n"
			os.sys.stderr.write(msq)
			exit(CLI.EX_WRONG_USAGE)
	
		if args.outpath:
			outdir = os.path.dirname(args.outpath)
			if not os.path.exists(outdir):
				msg = "Directory " + outdir + " does not exist\n"
				os.sys.stderr.write(msg)
				exit(CLI.EX_CANT_CREATE)
			if not os.access(outdir, os.W_OK):
				msg = "Output directory " + outdir + " not writable\n"
				os.sys.stderr.write(msg) 
				exit(CLI.EX_CANT_CREATE)


		#=======================================================================
		# The work...
		#=======================================================================
		shelf = shelve.open(SHELF_FILE, protocol=pickle.HIGHEST_PROTOCOL)
		doc = None
		ctxt = None
		try:
			doc = libxml2.parseFile(args.record)
			ctxt = doc.xpathNewContext()
			for ns in NAMESPACES.keys():
				ctxt.xpathRegisterNs(ns, NAMESPACES[ns])

			if args.subjects:
				if args.recursive: xpath = XPaths.SUBJECTS_RECURSIVE
				else: xpath = XPaths.SUBJECTS	
				_update_headings(xpath, ctxt, shelf, annotate=args.annotate, verbose=args.verbose)
			if args.names:
				if args.recursive: xpath = XPaths.NAMES_RECURSIVE
				else: xpath = XPaths.NAMES	
				_update_headings(xpath, ctxt, shelf, annotate=args.annotate, verbose=args.verbose)
			if args.outpath == None:
				os.sys.stdout.write(doc.serialize("UTF-8", 1))
			else:
				doc.saveFormatFileEnc(args.outpath, "UTF-8", 1)
			# if we got here...
			status = CLI.EX_OK

		#=======================================================================
		# Problems while doing "the work" are handled w/ Exceptions
		#=======================================================================
		except libxml2.parserError, e:
			os.sys.stderr.write(str(e.message) + "\n")
			status = CLI.EX_DATA_ERR

		except IOError, e:
			os.sys.stderr.write(str(e.message) + "\n")
			status = CLI.EX_IOERR

		except LookupError, e:
			os.sys.stderr.write(str(e.message))
			status = CLI.EX_SOFTWARE
					
		except Exception, e:
			os.sys.stderr.write(str(e.message) + "\n")
			status = CLI.EX_SOMETHING_ELSE
		
		finally:
			# clean up!
			shelf.close()
			if ctxt != None: ctxt.xpathFreeContext()
			if doc != None: doc.freeDoc()
			exit(status)
		 

if __name__ == "__main__": CLI()
