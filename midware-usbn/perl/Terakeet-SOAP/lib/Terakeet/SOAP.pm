# $Id: SOAP.pm,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $
package Terakeet::SOAP;
use strict;

use vars qw($VERSION @ISA @EXPORT);

BEGIN {
        require XML::DOM;
	require IO::Socket;
	require Digest::MD5;
        $VERSION = '0.1';

        my $XMLDOMminver = '1.43';
        die "installed version $XML::DOM::VERSION of XML::DOM < required ".
            "version $XMLDOMminver" unless $XML::DOM::VERSION >= $XMLDOMminver;
	my $IOSminver = '1.18';
	die "installed version $IO::Socket::VERSION of IO::Socket < required ".
	    "version $IOSminver" unless $IO::Socket::VERSION >= $IOSminver;

        #@ISA = qw(Exporter);
        #@EXPORT = qw();
}

# all modules must end with 1;
1;
