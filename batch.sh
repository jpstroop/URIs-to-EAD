#!/bin/bash

# Batch update EADs using addauths.py. Works recursively on a dir that is
# passed as the first and only argument. (this script is very cheap)

FIND=/usr/bin/find
PYTHON=/usr/bin/python
TMP_OUT=/tmp/out.xml
XMLLINT=/usr/bin/xmllint
MV=/bin/mv
for record in $($FIND $1 -name "*.xml" -type f -and ! -name ".*"); do
	echo $record
	# add the URIs to the temporary copy
	$PYTHON ./addauths.py -nsar -o $TMP_OUT $record

	# check the temporary copy (just try to parse it)
	$XMLLINT --noout $TMP_OUT

	# if all is OK accoding to xmllint, overwrite the source record
	if [ $? == "0" ]; then
		$MV $TMP_OUT $record
	else
		echo "Well formed check for $record returned status $?"
		echo "See http://xmlsoft.org/xmllint.html for Error Return Codes"
	fi
done

