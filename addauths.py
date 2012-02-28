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
		@param items: A list of 2-tuple (uri, label) possibilities
		"""
		self.heading = items
		self.type = items
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
# _normalize_heading
#===============================================================================
def _normalize_heading(heading):

	"""
	@param heading: A heading from the source data.
	@return: A normalized version of the heading.
	 
	@note: 	Other users may need to modify or extend this function. This v
	ersion, in order:
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
			# (if count is 1, casts to True)
			if bool(ctxt.xpathEval("count(//title[. = '"+name+"'])")): 
				label = ctxt.xpathEval("//title[. = '"+name+"']")[0].content
				uri = ctxt.xpathEval("//item[title[. = '"+name+"']]/link")[0].content
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
		
#	except Exception as e:
#		raise e
	
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
	
#	try: # if loc will change the 302 to include x-preflabel we can set allow_redirects to False and below to 302 
	if resp.status_code == 200:
		uri = resp.headers["x-uri"]
		label = resp.headers["x-preflabel"]
		return uri, label
	elif resp.status_code == 404:
		msg = "Not found " + subject + os.linesep
		raise HeadingNotFoundException(msg)
	else: # resp.status_code != 404 and status != 200:
		if callno: msg = "(" + callno + "): "
		else: msg = ""
		msg += " Response for \"" + subject + "\" was " + \
			resp.status_code + os.linesep
		raise UnexpectedResponseException(msg)
#	except:
#		raise

def _pers_or_corp_from_node(node):
	if node.get_name() == "corpname": return CORPORATE
	else: return PERSONAL
	
#===============================================================================
# update_headings
#===============================================================================
def update_headings(type, ctxt, shelf, callno="", recursive=False, annotate=False):
	
	if type == NAME and not recursive:
		xpath = "/ead:ead/ead:archdesc/ead:controlaccess/ead:corpname" + \
					"[not(@authfilenumber)]|" + \
				"/ead:ead/ead:archdesc/ead:controlaccess/ead:famname" + \
					"[not(@authfilenumber)]|" + \
				"/ead:ead/ead:archdesc/ead:controlaccess/ead:persname" + \
					"[not(@authfilenumber)]|" + \
				"/ead:ead/ead:archdesc/ead:did/ead:origination/*" + \
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
				if type == NAME:
					uri, auth = query_viaf(heading, _pers_or_corp_from_node(node))
					node.setProp("authfilenumber", uri)
				else: # type == SUBJECT; We've already checked values when choosing XPaths
					uri, auth = query_lc(heading)
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
			
#		except:
#			raise
		
class CLI(object):
	def __init__(self):
		desc = "Adds id.loc.gov URIs to subject headings and/or VIAF URIs to name " + \
				 "headings when established forms can be found."
		
		epi = "Exit status codes: 0 OK; 1 The input file or ouput directory (if -o) doesn't exist; 9: Something else went wrong\r\n"
	
		oHelp = "Path to the output file. Writes to stdout if no option is supplied."
		
		rHelp = "Recurse through the dsc. By default only the archdesc is treated."
		
		nHelp = "Link names."
		
		sHelp = "Link subjects."
	
		AHelp = "Annotate the record. When multiple matches are found XML comments" + \
			"containing the matches and their URIs will be added to the " + \
			"record."
	
		too_many = "Too many arguments supplied. Please supply the path " + \
					"to an EAD record."
		not_enough = "Not enough arguments supplied. Please supply the path " + \
					"to an EAD record."
		
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
			exit(1)
		
		# catch if -o and output dir does not exist
		if args.outpath:
			outdir = os.path.dirname(args.outpath)
			if not os.path.exists(outdir):
				os.sys.stderr.write("Directory " + outdir + " does not exist\n") 
				exit(1)
		
		shelf = shelve.open(SHELF_FILE, protocol=pickle.HIGHEST_PROTOCOL)
			
		try:
			doc = libxml2.parseFile(args.record)
			ctxt = doc.xpathNewContext()
			for ns in NAMESPACES.keys():
				ctxt.xpathRegisterNs(ns, NAMESPACES[ns])
			
			callno = ctxt.xpathEval("//ead:eadid")[0].content
			
			if not args.names and not args.subjects:
				status = 64
				raise Exception("Supply -n and or -s to link headings. Use --help for more details.")
			
			if args.subjects:
				update_headings(SUBJECT, ctxt, shelf, callno=callno, recursive=args.recursive, annotate=args.annotate)
			if args.names:
				update_headings(NAME, ctxt, shelf, callno=callno, recursive=args.recursive, annotate=args.annotate)
			
			if args.outpath == None:
				os.sys.stdout.write(doc.serialize("UTF-8", 1))
			else:
	#			doc.saveFileEnc()
				doc.saveFormatFileEnc(args.outpath, "UTF-8", 1)
			status = 0
		except UnicodeEncodeError, e:
			os.sys.stderr.write(str(e) + "\n")
		except Exception, e:
			os.sys.stderr.write(str(e) + "\n")
			status = 9
		
		finally:
			# clean up!
			shelf.close()
			ctxt.xpathFreeContext()
			doc.freeDoc()
			exit(status)
		 

if __name__ == "__main__":
	CLI()