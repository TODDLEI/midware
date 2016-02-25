#!/usr/bin/perl
# assembles SOAP messages based on parameters provided by the caller.
#
# $Id: Client.pm,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $
package Terakeet::SOAP::Client;
use strict;
use IO::Socket::INET;
use IO::Socket::SSL;
use Terakeet::SOAP::Message;
use Digest::MD5 qw(md5_hex);
use XML::DOM;

# class constructor
sub new {
	my $self = shift;

	# don't allow new() to be called on an existing object
	unless (ref $self) {
		my (@methods, @params);
		my $class = ref($self) || $self;
		# break our args into method calls (and their values) and
		# parameters.  idea from SOAP::Transport::TCP source.
		while (@_) {
			if ($class->can($_[0])) {
				# this param is a method call, so grab its
				# arguments.
				push @methods, shift() => shift();
			} else {
				# this param is a regular parameter to this
				# method, so accept it as it is.
				push @params, shift();
			}
		}

		# blame SOAP::Lite's author for this next block...
		$self = bless {@params}, $class;
		while (@methods) {
			my($method, $params) = splice(@methods,0,2);
			$self->$method((ref($params) eq 'ARRAY') ? @$params : $params);
		}
	}

	return $self;
}

# set/get whether we'll use SSL for outgoing connections
sub SSL {
	my $self = shift;
	if (@_) {
		$self->{_SSL} = shift;
		return $self;
	}
	return $self->{_SSL};
}

# set/get the path to an SSL client certificate
sub SSLCert {
	my $self = shift;
	if (@_) {
		$self->{_SSL_CERT} = shift;
		return $self;
	}
	return $self->{_SSL_CERT};
}

# set/get the path to an SSL CA certificate
sub SSLCA {
	my $self = shift;
	if (@_) {
		$self->{_SSL_CA} = shift;
		return $self;
	}
	return $self->{_SSL_CA};
}

# set/get whether we'll output debugging information
sub DEBUG {
	my $self = shift;
	if (@_) {
		$self->{_DEBUG} = shift;
		return $self;
	}
	return $self->{_DEBUG};
}

# set/get the host or IP address to contact
sub Host {
	my $self = shift;
	if (@_) {
		$self->{_HOST} = shift;
		return $self;
	}
	return $self->{_HOST};
}

# set/get the TCP port to contact
sub Port {
	my $self = shift;
	if (@_) {
		$self->{_PORT} = shift;
		return $self;
	}
	return $self->{_PORT};
}

# set/get whether we should wait on-line for answers to our requests
sub Wait {
	my $self = shift;
	if (@_) {
		$self->{_WAIT} = shift;
		return $self;
	}
	return $self->{_WAIT};
}

# explicitly connect to the server; also require an explicit disconnect later.
sub connect {
	my $self = shift;
	$self->{RQ_DISCONNECT} = 1;
	my $DEBUG = $self->{_DEBUG};

	my $Host = $self->Host;
	my $Port = $self->Port;
	my $SSL  = $self->SSL;
	my $Wait = $self->Wait;
	die "Need host and port" unless ($Host && $Port);

	my $sock;
	if ($SSL) {
		my $cert = $self->SSLCert;
		my $ca = $self->SSLCA;
		warn "Trying to create SSL socket" if $DEBUG;
		$sock = IO::Socket::SSL->new(
				PeerAddr	=> $Host,
				PeerPort	=> $Port,
				Proto		=> 'tcp',
				Timeout		=> 10,
				SSL_use_cert	=> ($cert ? 1 : 0),
				SSL_key_file	=> ($cert || undef),
				SSL_cert_file	=> ($cert || undef),
				SSL_ca_file	=> ($ca || undef),
				);
		warn "Created SSL socket" if $DEBUG;
	} else {
		$sock = IO::Socket::INET->new(
				PeerAddr	=> $Host,
				PeerPort	=> $Port,
				Proto		=> 'tcp',
				Timeout		=> 10,
				);
	}

	if (<$sock> =~ /^\+ READY\b/) {
		$self->{__SOCKET} = $sock;
		return 1;
	} else {
		return 0;
	}
}

# explicitly disconnect from the server.
sub disconnect {
	my $self = shift;

	# disconnect not required, so this operation is not defined
	return undef unless $self->{RQ_DISCONNECT};

	my $sock;
	if ($sock = $self->{__SOCKET}) {
		if ($sock->connected) {
			print $sock "QUIT\n";
			## server will close the socket, so we don't have to undef it.
			# undef $self->{__SOCKET};
		} else {
			delete $self->{__SOCKET};
		}
	}
}

# call a method.
sub call_method {
	my $self = shift;
	my $method = shift;
	my %params = @_;
	my $DEBUG = $self->{_DEBUG};
	my $WAIT = $self->{_WAIT};

	my $sock;
	my $need_disconnect = 0;
	if (($sock = $self->{__SOCKET}) && $sock->connected) {

		# assemble a SOAP message with the given method name and
		# parameters.  the Message object's constructor takes care of
		# unpacking the parameter hash's complexity, so we needn't
		# worry about doing so here.
		my $message = Terakeet::SOAP::Message->new(
			methodName => $method,
			methodArgs => \%params,
		);

		# issue the command and its contents to the server
		my $msgstr = $message->asString."\n";
		my $cksum = md5_hex($msgstr);
		my $waitmsg = ($WAIT ? " WILL_WAIT" : undef);
		print $sock "SOAP $cksum$waitmsg\n";
		print $sock $msgstr,"&\n";

		if ($WAIT) {
			my $qidline = <$sock>;
			chomp $qidline;
			if ($qidline =~ /^\+ OK qid=([A-F0-9a-f]{32},\d+)$/) {
				warn "Server queued message as $1" if $DEBUG;
			}
		}

		my $respstatus = <$sock>;
		chomp $respstatus;
		if ($respstatus =~ /^\+ OK ([A-F0-9a-f]{32})$/) {
			my $want_cksum = $1;
			my ($respxml, $line);
			while ($line = <$sock>) {
				last if ($line =~ /^&/);
				$respxml .= $line;
			}
			my $got_cksum = md5_hex($respxml);
			if ($want_cksum eq $got_cksum) {
				my $parser = XML::DOM::Parser->new();
				warn "building SOAP message for response" if $DEBUG;
				my $doc = $parser->parse($respxml);
				warn (defined($doc) ? 'respxml parsed into a '.ref($doc)
						    : 'respxml did not parse')
					if $DEBUG;
				my $msgn = Terakeet::SOAP::Message->new(
						Document => $doc,
						DEBUG => $DEBUG,
						);
				warn "finished building SOAP message: ".$msgn->asString
					if $DEBUG;
				return $msgn;
			} else {
				die "Bad checksum received";
			}
		} elsif ($respstatus =~ /^\- ERR\s?(.*)$/) {
			die ($1 ? "Error: $1" : "System error");
		} else {
			die "Unexpected response: '$respstatus'";
		}
	} else {
		$self->connect && $self->call_method($method, %params);
	}

	# harmless, good idea anyway:
	$self->disconnect;
}

sub DESTROY {
	my $self = shift;
	$self->disconnect;
}

# all modules must return 1 so that 'use'/'require' work.
1;
