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


NAME = "name"
SUBJECT = "subject"
CORPORATE = "corporate"
PERSONAL = "personal"

MADS_NAME_SCHEME_URI = "http://id.loc.gov/authorities/names"
MADS_SUBJECT_SCHEME_URI = "http://id.loc.gov/authorities/subjects"

ID_SUBJECT_RESOLVER = "http://id.loc.gov/vocabulary/subject/label/"
VIAF_SEARCH = "http://viaf.org/viaf/search"

RSS_XML = "application/rss+xml" 
APPLICATION_XML = "application/xml"

SHELF_FILE = "cache.shelf"

class HeadingNotFoundException(Exception): pass
class UnexpectedResponseException(Exception): pass
#===============================================================================
# MultipleMatchesException
#===============================================================================
class MultipleMatchesException(Exception):
	def __init__(self, heading, type, items):
		"""
		@param heading: The heading we were searching when this was raised
		@param type: The type of heading (personal or corporate)  
		@param items: A list of 2-tuple (uri, label) possibilities
		"""
		self.heading = heading
		self.type = type
		self.items = items

#===============================================================================
# Heading
#===============================================================================
class Heading(object):
	def __init__(self):
		"Heading label (string) normalized from the source data"
		self.value = ""
		"'name' or 'subject'"
		self.type = ""
		"boolean, True if one or more URIs was found"
		self.found = ""
		"A list of 2-tuple (uri, label) possibilities"
		self.alternatives = ""

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
	@param type: "personal" or "corporate"
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
		doc = libxml2.parseDoc(resp.text.encode("UTF-8"))
		ctxt = doc.xpathNewContext()
		for ns in NAMESPACES.keys():
			ctxt.xpathRegisterNs(ns, NAMESPACES[ns])

		count = int(ctxt.xpathEval("//opensearch:totalResults")[0].content)
		
		if count == 1:
			uri = ctxt.xpathEval("//link[parent::item]")[0].content
			label = ctxt.xpathEval("//title[parent::item]")[0].content
			return (uri, label)
		elif count == 0:
			msg = "Not found " + name + os.linesep
			raise HeadingNotFoundException(msg)	
		elif count > 1:
			# check for an exact match, we'll return that
			if bool(ctxt.xpathEval("count(//title[. = '" + name + "'])")):
				# (re. above magic: if count is 1, casts to True) 
				label = ctxt.xpathEval("//title[. = '" + name + "']")[0].content
				uri = ctxt.xpathEval("//item[title[. = '" + name + "']]/link")[0].content
				return (uri, label)
			else:
				# We make a list of (uri, authform ) two-tuples that the 
				# exception can report.
				msg = "Multiple matches for " + name
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
				raise MultipleMatchesException(name, type, items)
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
		msg = "Not found " + subject + os.linesep
		raise HeadingNotFoundException(msg)
	else: # resp.status_code != 404 and status != 200:
		msg = " Response for \"" + subject + "\" was "
		msg += resp.status_code + os.linesep
		raise UnexpectedResponseException(msg)

#===============================================================================
# _pers_or_corp_from_node
#===============================================================================
def _pers_or_corp_from_node(node):
	# TODO: make sure that node.get_name() returns the local name, and not, 
	# e.g. ead:corpname when there is a namespace prefix
	if node.get_name() == "corpname": return CORPORATE
	else: return PERSONAL
	
#===============================================================================
# update_headings
#===============================================================================
def update_headings(xpath, ctxt, shelf, annotate=False):
	
	for node in ctxt.xpathEval(xpath):
		try:
			heading = _normalize_heading(node.content)
			
			# Check the shelf right off
			if heading in shelf and shelf.get(heading).type == type:
				cached = shelf[heading]
				if len(cached.alternatives) == 1: 
					uri = cached.alternatives[0][0]
					node.setProp("authfilenumber", uri)
				elif len(cached.alternatives) > 1 and annotate:
					commentContent = os.linesep + "Possible URIs:" + os.linesep
					for alt in cached.alternatives:
						commentContent += alt[0] + " : " + alt[1] + os.linesep 
					comment = libxml2.newComment(commentContent)
					node.addNextSibling(comment)
			else:
				if node.get_name() == "subject":
					uri, auth = query_lc(heading)
					node.setProp("authfilenumber", uri)
				else:
					uri, auth = query_viaf(heading, _pers_or_corp_from_node(node))
					node.setProp("authfilenumber", uri)

				# we put the heading we found in the db
				record = Heading()
				record.value = heading
				record.type = type
				record.found = True
				record.alternatives = [(uri, auth)]
				shelf[heading] = record

				sleep(1) # A courtesy to the services.
			
			node.setProp("authfilenumber", uri)
		
		except (HeadingNotFoundException, UnexpectedResponseException), e:
			os.sys.stderr.write(str(e))
		
		except MultipleMatchesException, m:
			if annotate:
				content = os.linesep + "Possible URIs:" + os.linesep
				for alt in m.items:
					content += alt[0] + " : " + alt[1] + os.linesep 
				comment = libxml2.newComment(content)
				node.addNextSibling(comment)
			# We still want to put this in the db
			record = Heading()
			record.value = m.heading
			record.type = m.type
			record.found = True
			record.alternatives = m.items
			shelf[heading] = record
		
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
		
	EX_CANT_CREATE = 73
	"""User specified output file cannot be created."""
	
	EX_IOERR = 74
	"""An error occurred while doing I/O on some file."""
		
	def __init__(self):
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
	
		AHelp = "Annotate the record. When multiple matches are found XML " + \
			"comments containing the matches and their URIs will be added " + \
			"to the record."

		parser = ArgumentParser(description=desc, epilog=epi)
		parser.add_argument("-o", "--output", default=None, required=False, dest="outpath", help=oHelp)
		parser.add_argument("-r", "--recursive", default=False, required=False, dest="recursive", action="store_true", help=rHelp)
		parser.add_argument("-n", "--names", default=False, required=False, dest="names", action="store_true", help=nHelp)
		parser.add_argument("-s", "--subjects", default=False, required=False, dest="subjects", action="store_true", help=sHelp)
		parser.add_argument("-a", "--annotate", default=False, required=False, dest="annotate", action="store_true", help=AHelp)
		parser.add_argument("record", default=None)
		args = parser.parse_args()

		 # catch if input file does not exist
		if not os.path.exists(args.record):
			os.sys.stderr.write("File " + args.record + " does not exist\n")
			exit(CLI.EX_NO_INPUT)
			
		if args.record == None:
			os.sys.stderr.write("No input file supplied. See --help for usage")
			exit(CLI.EX_WRONG_USAGE)
			
		# catch if -o and output dir does not exist
		if args.outpath:
			outdir = os.path.dirname(args.outpath)
			if not os.access(outdir, os.W_OK):
				msg = "Output directory " + outdir + " not writable\n"
				os.sys.stderr.write(msg) 
				exit(CLI.EX_CANT_CREATE)
			if not os.path.exists(outdir):
				msg = "Directory " + outdir + " does not exist\n"
				os.sys.stderr.write(msg) 
				exit(CLI.EX_CANT_CREATE)

		shelf = shelve.open(SHELF_FILE, protocol=pickle.HIGHEST_PROTOCOL)

		doc = None
		ctxt = None
		try:
			doc = libxml2.parseFile(args.record)
			ctxt = doc.xpathNewContext()
			for ns in NAMESPACES.keys():
				ctxt.xpathRegisterNs(ns, NAMESPACES[ns])

			if not args.names and not args.subjects:
				status = CLI.EX_WRONG_USAGE
				msg = "Supply -n and or -s to link headings. Use --help " + \
				"for more details."
				raise Exception(msg)
					
			if args.subjects:
				if args.recursive: xpath = XPaths.SUBJECTS_RECURSIVE
				else: xpath = XPaths.SUBJECTS	
				update_headings(xpath, ctxt, shelf, annotate=args.annotate)
			if args.names:
				if args.recursive: xpath = XPaths.NAMES_RECURSIVE
				else: xpath = XPaths.NAMES	
				update_headings(xpath, ctxt, shelf, annotate=args.annotate)

			if args.outpath == None:
				os.sys.stdout.write(doc.serialize("UTF-8", 1))
			else:
				doc.saveFormatFileEnc(args.outpath, "UTF-8", 1)
			status = CLI.EX_OK

		except libxml2.parserError, e:
			os.sys.stderr.write(str(e) + "\n")
			status = CLI.EX_DATA_ERR

		except IOError, e:
			os.sys.stderr.write(str(e) + "\n")
			status = CLI.EX_IOERR
			
		except Exception, e:
			os.sys.stderr.write(str(e) + "\n")
			status = CLI.EX_SOMETHING_ELSE
		
		finally:
			# clean up!
			shelf.close()
			if ctxt != None: ctxt.xpathFreeContext()
			if doc != None: doc.freeDoc()
			exit(status)
		 

if __name__ == "__main__": CLI()
