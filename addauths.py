#!/usr/bin/env python
#-*- coding: utf-8 -*-

import libxml2
import os
import urllib2
import httplib
import shelve
import pickle
from time import sleep
from argparse import ArgumentParser
#from optparse import OptionParser
from sys import exit

NAMESPACES = {
	"ead":"urn:isbn:1-931666-22-9",
	"xlink":"http://www.w3.org/1999/xlink",
	"rdf":"http://www.w3.org/1999/02/22-rdf-syntax-ns#",
	"madsrdf":"http://www.loc.gov/mads/rdf/v1#"
}


NAME = "name"
SUBJECT = "subject"
ID = "id.loc.gov"
ID_NAME_RESOLVER = "/vocabulary/name/label/"
MADS_NAME_SCHEME_URI = "http://id.loc.gov/authorities/names"
ID_SUBJECT_RESOLVER = "/vocabulary/subject/label/"
MADS_SUBJECT_SCHEME_URI = "http://id.loc.gov/authorities/subjects"
SHELF_FILE="cache.db"

class WrongSchemeException(Exception): pass
class HeadingNotFoundException(Exception): pass
class UnexpectedResponseException(Exception): pass

class CachedHeading(object):
	def __init__(self, type, uri, auth):
		self.type=type
		self.uri=uri
		self.auth=auth

def _normalize_heading(heading):
	collapsed = " ".join(heading.split()).replace(" -", "-").replace("- ", "-")
	if collapsed.endswith("."):
		stripped = collapsed[:-1]
	else:
		stripped = collapsed	 
	return stripped

def get_uri_and_auth_form(type, heading):
	'''
	@param type: "name" or "subject"
	@type type: string 
	
	@param heading: a name or subject heading
	@type heading: string
	
	@raise WrongSchemeExcaption: when the heading is found but not in the
		expected scheme
		
	@raise HeadingNotFoundException: when the heading isn't found
	
	@raise UnexpectedResponseException: when the initial response from LC is not 
		a 302 or 404 (404 should raise a HeadingNotFoundException)
	
	'''
	uri=""
	auth=""
	
	if type == NAME: 
		resolver = ID_NAME_RESOLVER
		scheme = MADS_NAME_SCHEME_URI
	elif type == SUBJECT: 
		resolver = ID_SUBJECT_RESOLVER
		scheme = MADS_SUBJECT_SCHEME_URI
	else: 
		raise ValueError("Unknown type of heading. Must be one of " + \
			"(\"name\", \"subject\"). Supplied: \"" + type + "\"")
		
	headers = {"Accept":"application/xml"}
	
	conn = httplib.HTTPConnection(ID)
	try:
		url = resolver + urllib2.quote(heading)
		conn.request("GET", url, headers=headers)
		response = conn.getresponse()
	except:
		raise
	finally:
		conn.close()
		
	sleep(1)
	
	status = response.status
	if status == 302:
		location = response.getheader("location", None)
		req = urllib2.Request(location)
		req.add_header("Accept", "application/rdf+xml")
		r = urllib2.urlopen(req)
		
		#print r.read()
		
		doc = libxml2.parseDoc(r.read())
		ctxt = doc.xpathNewContext()
		
		for ns in NAMESPACES.keys(): ctxt.xpathRegisterNs(ns, NAMESPACES[ns])
		
		scheme_query = "/rdf:RDF/*/madsrdf:isMemberOfMADSScheme" +\
						"[@rdf:resource='" + scheme + "']"
		scheme_correct = ctxt.xpathEval(scheme_query)
		
		if not scheme_correct: # empty lists cast to False
			msg = callno + ": Record found for " + heading + " not in " +\
				scheme + " scheme" + os.linesep
			raise WrongSchemeException(msg)
		
		auth_xpath = "/rdf:RDF/*/madsrdf:authoritativeLabel[1]"
		auth = ctxt.xpathEval(auth_xpath)[0].content
		
		uri_xpath="/rdf:RDF/*/@rdf:about"
		uri=ctxt.xpathEval(uri_xpath)[0].content
		
		# clean up!
		ctxt.xpathFreeContext()
		doc.freeDoc()
		
		return uri, auth
		
	elif status == 404:
		msg = "Not found (" + callno + ") " + heading + os.linesep
		raise HeadingNotFoundException(msg)
	elif status != 404 and status != 302:
		msg = callno + ": Response for \"" + heading + "\" was " +\
			response.status + os.linesep
		raise UnexpectedResponseException(msg)
	else:
		raise

def update_headings(type, ctxt, shelf, callno=""):
	'''
	@param type: "name" or "subject"
	@type type: string
	@param ctxt: an xpath context
	@type ctxt: libxml2.xpathContext 
	
	'''
	if type == NAME:
		xpath = "//ead:archdesc/ead:controlaccess/ead:corpname" +\
					"[not(@authfilenumber)]|"+\
				"//ead:archdesc/ead:controlaccess/ead:famname"+\
					"[not(@authfilenumber)]|"+\
				"//ead:archdesc/ead:controlaccess/ead:persname"+\
					"[not(@authfilenumber)]|"+\
				"//ead:archdesc/ead:did/ead:origination/*"
	elif type == SUBJECT:
		xpath = "//ead:archdesc/ead:controlaccess/ead:subject"+\
					"[not(@source = 'local') and not(@authfilenumber)]"
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
			
			# TODO: add something to the cache to indicate a heading wasn't found.
			
			node.setProp("authfilenumber", uri)
			
			# update the value if approprate
			if heading != auth:
				os.sys.stderr.write("Changed \"" + heading + "\" to \"" +\
						auth + "\"")
				node.setContent(auth)
				cmt = libxml2.newComment("Content was: " + heading)
				node.addNextSibling(cmt)
				
			sleep(1)
		
		# TODO: Why can't we pass these down to main. Read up...	
		except (WrongSchemeException, HeadingNotFoundException, \
						UnexpectedResponseException), e:
			os.sys.stderr.write(str(e))
			
		except:
			raise

if __name__ == "__main__":
	
	desc="Adds id.loc.gov URIs to headings when established forms can be found."

	epi="Exit status codes: 0 OK; 1 The input file or ouput directory (if -o) doesn't exist; 9: Something else went wrong\r\n"

	oHelp="Path to the output file. Writes to stdout if no option is supplied."

	too_many = "Too many arguments supplied. Please supply the path "+\
					"to an EAD record."
	not_enough = "Not enough arguments supplied. Please supply the path "+\
						"to an EAD record."

#	parser = OptionParser(usage)
#	parser.add_option("-o", "--output", default=None, dest="outpath", help=oHelp)
#	(options, args) = parser.parse_args()
	
	parser = ArgumentParser(description=desc, epilog=epi)
	parser.add_argument("-o", "--output", default=None, required=False, dest="outpath", help=oHelp)
	parser.add_argument("record", default=None)
	args = parser.parse_args()
	
	 # cath if input file does not exist
	if not os.path.exists(args.record):
		os.sys.stderr.write("File " + args.record + " does not exist\n")
		exit(1)
	
	# catch if -o and output dir does not exist
	if args.outpath:
		outdir = os.path.dirname(args.outpath)
		if not os.path.exists(outdir):
			os.sys.stderr.write("Directory " + outdir + " does not exist\n") 
			exit(1)
	
	shelf=shelve.open(SHELF_FILE, protocol=pickle.HIGHEST_PROTOCOL)
	try:
		doc = libxml2.parseFile(args.record)
		ctxt = doc.xpathNewContext()
		for ns in NAMESPACES.keys():
			ctxt.xpathRegisterNs(ns, NAMESPACES[ns])
		
		callno=ctxt.xpathEval("//ead:eadid")[0].content
		
		update_headings(SUBJECT, ctxt, shelf, callno)
		update_headings(NAME, ctxt, shelf, callno)
		
		if args.outpath == None:
			os.sys.stdout.write(doc.serialize("UTF-8", 1))
		else:
			doc.saveFileEnc(args.outpath, "UTF-8")
		status=0
		
	except Exception, e:
		os.sys.stderr.write(str(e))
		status=9
	
	finally:
		# clean up!
		shelf.close()
		ctxt.xpathFreeContext()
		doc.freeDoc()
		exit(status)