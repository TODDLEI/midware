#!/bin/sh
# $Id: runme.sh,v 1.2 2008/04/09 20:30:10 aastle Exp $

# Java can't always figure out which entropy source to use, so help it a little
# if we're running on FreeBSD.
RANDOMNESS=""
if [ "x`uname`" = "xFreeBSD" ]; then
	RANDOMNESS="-Djava.security.egd=file:/dev/urandom"
fi

if [ ! -f MessageListener.props ]; then
	echo "No properties file for MessageListener."
	exit 1
fi

if [ ! -f log4j.props ]; then
	echo "No log4j properties file."
	exit 2
fi

#	-Djavax.net.ssl.trustStore=security/theTruststore.truststore \
java -server -cp premier.jar:. \
	$RANDOMNESS \
	net.terakeet.soapware.MessageListener
