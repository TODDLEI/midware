#!/bin/sh
# $Id: loadtest.sh,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $

if [ "$#" != "3" ]; then
	echo "Usage: `basename $0` \"host1:port1 [host2:port2 [...]]\" /path/to/test/descriptors /path/to/sample/requests"
	exit 1
fi

HOSTS=$1
TESTDESCDIR=$2
REQPATH=$3

BUILDDIR=`pwd`/../build

cd "$TESTDESCDIR" || exit 2;
TESTDESCDIR=`pwd`

for HOSTPORT in $HOSTS
do (\
	HOST=`echo "${HOSTPORT}" | sed -e 's/^\(.*\):.*/\1/'`
	PORT=`echo "${HOSTPORT}" | sed -e 's/^.*:\(.*\)$/\1/'`
	FILENAME="run-${HOST}@${PORT}-`date +%s`.txt"
	for TEST in *.xml ; do (\
		echo -n "Running test ${TEST}... "; \
		(cd "$BUILDDIR" && \
		java net.terakeet.test.LoadTester "$HOST" $PORT 0 "$TESTDESCDIR"/"$TEST" "$REQPATH" \
		 ) >> "$FILENAME"; \
		echo "done."; \
	); \
	done \
)
done
