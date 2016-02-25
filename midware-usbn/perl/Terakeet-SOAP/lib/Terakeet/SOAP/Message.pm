#!/usr/bin/perl
# assembles SOAP messages based on parameters provided by the caller.
#
# $Id: Message.pm,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $
package Terakeet::SOAP::Message;
use strict;
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

# set/get the name of the SOAP method to call
sub methodName {
	my $self = shift;
	if (@_) {
		$self->{_methodName} = shift;
		return $self;
	}
	return $self->{_methodName};
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

# set/get the arguments to the SOAP method
sub methodArgs {
	my $self = shift;
	if (@_) {
		$self->{_methodArgs} = shift;
		return $self;
	}
	return $self->{_methodArgs};
}

# set/get the document we wrap around
sub Document {
	my $self = shift;
	my $DEBUG = $self->{_DEBUG};
	if (@_) {
		my $docobj = shift;
		if (ref($docobj) eq 'XML::DOM::Document') {
			$self->{_Document} = $docobj;
			warn "Built Terakeet::SOAP::Message from XML::DOM::Document."
				if $DEBUG;
		} else {
			warn "Tried to build Terakeet::SOAP::Message from something".
			     " other than an XML::DOM::Document object";
		}
		return $self;
	}
	return $self->{_Document};
}

# set/get the complex data representation of the Document
sub Data {
	my $self = shift;
	if (@_) {
		$self->{_Data} = shift;
		return $self;
	}
	return $self->{_Data};
}

sub asString {
	my $self = shift;
	my $mname = $self->methodName;
	my $margsref = $self->methodArgs;
	my $DEBUG = $self->{_DEBUG};
	my $doc;

	unless ($mname) {
		if ($doc = $self->Document) {
			return $doc->toString;
		} else {
			return undef;
		}
	}

	$doc = XML::DOM::Document->new();
	my $decl = $doc->createXMLDecl('1.0');
	$doc->setXMLDecl($decl);
	my ($envelope, $body);
	$envelope = $doc->createElement("SOAP-ENV:Envelope");
	$doc->appendChild($envelope);
	$envelope->setAttribute("xmlns:SOAP-ENV",
			"http://schemas.xmlsoap.org/soap/envelope/");
	$body = $doc->createElement("SOAP-ENV:Body");
	$envelope->appendChild($body);

	# create an XML::DOM::Element from the method name and method argument
	# hashtable
	my $mEl = eltify($doc,$body,"m:$mname",$margsref);
	if ($mEl) {
		$mEl->setAttribute("xmlns:m",
				"http://www.terakeet.net/schemas/usadMessage");
		$body->appendChild($mEl);
	}

	$self->Document($doc) if $doc;
	return $decl->toString() . "\n" . $envelope->toString();
}

# opposite of hashify
sub eltify {
	my $doc = shift;
	my $parent = shift;
	my $eltname = shift;
	my $vref = shift;
	if (ref($vref) eq 'ARRAY') {
		foreach my $v (@$vref) {
			$parent->appendChild( eltify($doc,$parent,$eltname,$v) );
		}
		# can't return $parent or else XML::DOM complains (with good
		# reason; an element can't be appended to itself as a child)
		return undef;
	} elsif (ref($vref) eq 'HASH') {
		# create an element with name $eltname and append a bunch of
		# children to it
		my $el = $doc->createElement($eltname);
		my ($k, $v);
		while (($k, $v) = each %$vref) {
			my $chel = eltify($doc,$el,$k,$v);
			$el->appendChild($chel) if $chel; 
		}
		return $el;
	} else { # scalar
		# return an element with name $eltname and value $vref
		# (disregard the parent element)
		my $el = $doc->createElement($eltname);
		$el->addText($vref);
		return $el;
	}
}

# turns an Element that contains nested text-containing Elements into a
# hashtable.  recursive but not tail-recursive.
sub hashify {
	my $elt = shift;
	return {} unless $elt;
	my $recursed = shift; # is this a recursive call?
	(my $mname = $elt->getTagName) =~ s/^[^:]+://;
	return {} unless $mname;
	if ($elt->hasChildNodes) {
		my $type;
		my %tmphash = ();
		foreach my $child ($elt->getChildNodes) {
			$type = $child->getNodeType;
			if ($type == XML::DOM::ELEMENT_NODE) {
				my $childName = $child->getTagName;
				my $already = $tmphash{$childName};
				# if we already have data for a given child,
				# turn the existing entity into an array (if it
				# isn't already) and push the new value.
				if ($already) {
					if (ref($already) ne 'ARRAY') {
						$tmphash{$childName} = [];
						push @{$tmphash{$childName}}, $already;
					}
					push @{$tmphash{$childName}}, hashify($child, 1);
				} else {
					# first time we've seen this child
					$tmphash{$childName} = hashify($child, 1);
				}
			} elsif ($type == XML::DOM::TEXT_NODE) {
				return $child->getNodeValue;
			}
		}
		# this return statement is slightly gross but necessary.  we
		# return our method name => result mapping only if we are the
		# first call in the chain.  that is, recursive calls don't
		# return such a mapping; they only return the value part.
		return ( $recursed ? \%tmphash : { $mname => \%tmphash } );
	} else {
		return ( $recursed ? undef : { $mname => undef } );
	}
}

# fetches a complex data structure representation of the Document
sub getData {
	my $self = shift;
	my $DEBUG = $self->{_DEBUG};
	my $doc = $self->Document;
	my $toplevel = {};

	return {} unless $doc;

	my $envelope = $doc->getDocumentElement;
	return undef unless $envelope;
	warn("processing " . $envelope->getTagName) if $DEBUG;
	if ($envelope->getTagName !~ /:?Envelope$/) { return {}; }
	my $body = $envelope->getFirstChild;
	warn("processing " . $body->getTagName) if $DEBUG;
	if ($body->getTagName !~ /:?Body$/) { return {}; }
	my $methodResults = $body->getFirstChild;
	(my $methodName = $methodResults->getTagName) =~ s/^[^:]+://;
	warn("processing " . $methodName) if $DEBUG;
	if ($methodName) {
		$toplevel = hashify($methodResults);
	}

	$self->Data($toplevel);
	return $toplevel;
}

sub DESTROY {
	my $self = shift;

	my $doc;
	if ($doc = $self->Document) {
		$doc->dispose;
	}
}

# all modules must end with 1;
1;
