#!/bin/sh

if [ ! "`whoami`" = "midware" ]; then
	echo "You must run the script as 'midware'"
	exit 1
fi

current=`pwd`
echo ""
echo "** Checking for running middleware"

if [ "$1" = "live" ]; then
	echo "** LIVE"
	dev=""
	live="live"
	jar="premier.jar"
	pid=`ps awux | grep premier\.jar | grep -v grep | sed -e's/^ *//' -e's/ \{1,\}/ /g' | cut -d' ' -f2`
else
	echo "** DEVELOPMENT"
	dev="/dev"
	live=""
	jar="premier-dev.jar"
	pid=`ps awux | grep premier-dev\.jar | grep -v grep | sed -e's/^ *//' -e's/ \{1,\}/ /g' | cut -d' ' -f2`
fi

cd /home/midware$dev

if [ ! "$pid" = "" ]; then
	echo "** Killing middleware process $pid"
	kill $pid
else
	echo "** No middleware process found"
fi

cd dist
echo ""
echo "** Starting middleware"

### Code from runme.sh
RANDOMNESS=""
if [ "x`uname`" = "xFreeBSD" ]; then
	RANDOMNESS="-Djava.security.egd=file:/dev/urandom"
fi

if [ ! -f MessageListener.props ]; then
	echo "No properties file for MessageListener."
	exit 2
fi

if [ ! -f log4j.props ]; then
	echo "No log4j properties file."
	exit 3
fi

#	-Djavax.net.ssl.trustStore=security/theTruststore.truststore \
java -server -cp $jar:. \
	$RANDOMNESS \
	net.terakeet.soapware.MessageListener \
	>/dev/null 2>/dev/null </dev/null &

echo ""
echo "** Done"

cd $current
