#!/usr/bin/env python
#-*- coding: utf-8 -*-
import libxml2
import os
import urllib2
import httplib
import shelve
import pickle
import requests
from time import sleep
from argparse import ArgumentParser
from sys import exit

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

MADS_NAME_SCHEME_URI = "http://id.loc.gov/authorities/names"
MADS_SUBJECT_SCHEME_URI = "http://id.loc.gov/authorities/subjects"


ID_SUBJECT_RESOLVER = "http://id.loc.gov/vocabulary/subject/label/"
VIAF_SEARCH = "http://viaf.org/viaf/search"
RSS_XML = "application/rss+xml" 
APPLICATION_XML = "application/xml"

SHELF_FILE = "cache.shelf"

class WrongSchemeException(Exception): pass
class HeadingNotFoundException(Exception): pass
class UnexpectedResponseException(Exception): pass
class MultipleMatchesException(Exception):
	"""
	@param items: A list of 2-tuple (uri, label) possibilities 
	"""
	def __init__(self, items):
		self.items = items

class CachedHeading(object):
	def __init__(self, type, uri, auth):
		self.type = type
		self.uri = uri
		self.auth = auth


def _normalize_heading(heading):
	"""
	Other users may need to modify or extend this function. This version, in 
	order:
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

def query_viaf(name, type, accept=RSS_XML):
	"""
	@param name: name to look for in VIAF 
	@param type: "personal" or "corporate"
	@param accept: MIME type for accept header
	@return: the body of the response 
	"""
	q = 'local.' + type + 'Names+%3D+"' + name + '"+and+local.sources+any+"lc"'
	
	headers = {'Accept': accept}
	params = {"query":q}
	resp = requests.get(VIAF_SEARCH, headers=headers, params=params)
	try:
		doc = libxml2.parseDoc(resp.text)
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
			# We make a list of (uri, authform ) two-tuples that the exception
			# can report.
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
			raise MultipleMatchesException(items)
		else:
			raise Exception("Could not retrieve count (" + name + ")")
	except:
		raise
	finally:
		# clean up!
		ctxt.xpathFreeContext()
		doc.freeDoc()
		
	 

# TODO: rewite! Don't worry about auth form; maybe split into sep for names and subjects
def query_lc(subject):
	'''
	@param heading: a name or subject heading
	@type heading: string
	
	@raise WrongSchemeExcaption: when the heading is found but not in the
		expected scheme
		
	@raise HeadingNotFoundException: when the heading isn't found
	
	@raise UnexpectedResponseException: when the initial response from LC is not 
		a 302 or 404 (404 should raise a HeadingNotFoundException)
	
	'''
	to_get = ID_SUBJECT_RESOLVER + subject
	headers = {"Accept":"application/xml"}
	resp = requests.get(to_get, headers=headers, allow_redirects=True)
	
	
	try: # if loc will change the 302 to include x-preflabel we can set allow_redirects to False and below to 302 
		if resp.status_code == 200:
			uri = resp.headers["x-uri"]
			label = resp.headers["x-preflabel"]
			return uri, label
		elif resp.status_code == 404:
			msg = "Not found " + heading + os.linesep
			raise HeadingNotFoundException(msg)
		else: # resp.status_code != 404 and status != 200:
			msg = callno + ": Response for \"" + heading + "\" was " + \
				resp.status_code + os.linesep
			raise UnexpectedResponseException(msg)
	except:
		raise
	

def update_headings(type, ctxt, shelf, callno="", recursive=False, annotate=False):
	'''
	@param type: "name" or "subject"
	@type type: string
	@param ctxt: an xpath context
	@type ctxt: libxml2.xpathContext 
	
	'''
	if type == NAME and not recursive:
		xpath = "//ead:archdesc/ead:controlaccess/ead:corpname" + \
					"[not(@authfilenumber)]|" + \
				"//ead:archdesc/ead:controlaccess/ead:famname" + \
					"[not(@authfilenumber)]|" + \
				"//ead:archdesc/ead:controlaccess/ead:persname" + \
					"[not(@authfilenumber)]|" + \
				"//ead:archdesc/ead:did/ead:origination/*" + \
					"[not(@authfilenumber)]"
	elif type == NAME and recursive:
		xpath = "//ead:corpname[not(@authfilenumber)]|" + \
				"//ead:famname[not(@authfilenumber)]|" + \
				"//ead:persname[not(@authfilenumber)]|" + \
				"//ead:origination/*[not(@authfilenumber)]"
				
	elif type == SUBJECT and not recursive:
		xpath = "//ead:archdesc/ead:controlaccess/ead:subject" + \
					"[not(@source = 'local') and not(@authfilenumber)]"
	elif type == SUBJECT and recursive:
		xpath = "//ead:subject[not(@source = 'local') and not(@authfilenumber)]"				
	else:
		raise ValueError("Unknown type of heading. Must be one of " + \
			"(\"name\", \"subject\"). Supplied: \"" + type + "\"")
		
	for node in ctxt.xpathEval(xpath):
		try:
			heading = _normalize_heading(node.content)
			
			# Check the shelf right off. If we have the heading cached we can
			# return the URI and the authorized form. Note that we need to check 
			# the type of the heading (NAME, SUBJECT) as well.
			if heading in shelf and shelf.get(heading).type == type:
				cached = shelf[heading]
				uri, auth = cached.uri, cached.auth
			else:
				uri, auth = get_uri_and_auth_form(type, heading)
				# we put the heading we found in the data in the db so that if
				# we find it again, we'll get the URI and the authorized form
				# (see above)
				shelf[heading] = CachedHeading(type, uri, auth)
				 
				sleep(.5) # A courtesy to the services.
			
			# TODO: add something to the cache to indicate a heading wasn't found.
			
			node.setProp("authfilenumber", uri)
		
		# TODO: Why can't we pass these down to main. Read up...	
		except (WrongSchemeException, HeadingNotFoundException, \
						UnexpectedResponseException), e:
			os.sys.stderr.write(str(e))
			
		except:
			raise

#class CLI(object):
#	desc = "Adds id.loc.gov URIs to subject headings and/or VIAF URIs to name " + \
#		 "headings when established forms can be found."
#
#	epi = "Exit status codes: 0 OK; 1 The input file or ouput directory (if -o) doesn't exist; 9: Something else went wrong\r\n"
#
#	oHelp = "Path to the output file. Writes to stdout if no option is supplied."
#	
#	rHelp = "Recurse through the dsc. By default only the archdesc is treated."
#	
#	nHelp = "Link names."
#	
#	sHelp = "Link subjects."
#
#	AHelp = "Annotate the record. When multiple matches are found XML comments" + \
#		"containing the matches and their URIs will be added to the " + \
#		"record."
#
#	too_many = "Too many arguments supplied. Please supply the path " + \
#				"to an EAD record."
#	not_enough = "Not enough arguments supplied. Please supply the path " + \
#				"to an EAD record."
#	
#	parser = ArgumentParser(description=desc, epilog=epi)
#	parser.add_argument("-o", "--output", default=None, required=False, dest="outpath", help=oHelp)
#	parser.add_argument("-r", "--recursive", default=False, required=False, dest="recursive", action="store_true", help=rHelp)
#	parser.add_argument("-n", "--names", default=False, required=False, dest="names", action="store_true", help=nHelp)
#	parser.add_argument("-s", "--subjects", default=False, required=False, dest="subjects", action="store_true", help=sHelp)
#	parser.add_argument("-a", "--annotate", default=False, required=False, dest="annotations", action="store_true", help=AHelp)
#	parser.add_argument("record", default=None)
#	args = parser.parse_args()
#	
#	 # cath if input file does not exist
#	if not os.path.exists(args.record):
#		os.sys.stderr.write("File " + args.record + " does not exist\n")
#		exit(1)
#	
#	# catch if -o and output dir does not exist
#	if args.outpath:
#		outdir = os.path.dirname(args.outpath)
#		if not os.path.exists(outdir):
#			os.sys.stderr.write("Directory " + outdir + " does not exist\n") 
#			exit(1)
#	
#	shelf = shelve.open(SHELF_FILE, protocol=pickle.HIGHEST_PROTOCOL)
#		
#	try:
#		doc = libxml2.parseFile(args.record)
#		ctxt = doc.xpathNewContext()
#		for ns in NAMESPACES.keys():
#			ctxt.xpathRegisterNs(ns, NAMESPACES[ns])
#		
#		callno = ctxt.xpathEval("//ead:eadid")[0].content
#		
#		if not args.names and not args.subjects:
#			raise Exception("Supply -n and or -s to link headings. Use --help for more details.")
#		
#		if args.subjects:
#			update_headings(SUBJECT, ctxt, shelf, callno, args.recursive, args.annotate)
#		if args.names:
#			update_headings(NAME, ctxt, shelf, callno, args.recursive, args.annotate)
#		
#		if args.outpath == None:
#			os.sys.stdout.write(doc.serialize("UTF-8", 1))
#		else:
#			doc.saveFileEnc(args.outpath, "UTF-8")
#		status = 0
#		
#	except Exception, e:
#		os.sys.stderr.write(str(e) + "\n")
#		status = 9
#	
#	finally:
#		# clean up!
#		shelf.close()
#		ctxt.xpathFreeContext()
#		doc.freeDoc()
#		exit(status)


if __name__ == "__main__":
	try:
		print query_viaf("Stevenson, Adlai", "personal")
		
	except MultipleMatchesException as m:
		for item in m.items:
			print item
		 
