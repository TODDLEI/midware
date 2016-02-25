# test script for Terakeet::SOAP
# $Id: 03elthash.t,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $

use Test;
BEGIN { plan tests=>4 };

use Data::Dumper;
use Terakeet::SOAP::Message;
use XML::DOM;

eval { require("t/settings.req"); } ||
eval { require("settings.req"); };

my $msg = Terakeet::SOAP::Message->new(
		methodName => $TEST_METHOD,
		methodArgs => \%TEST_PARAMS,
		DEBUG => 1,
		);
my $msgString = $msg->asString;
warn $msgString;
ok($msgString);

my $doc = $msg->Document;
my $docString = $doc->toString;
warn $docString;
ok($docString);

my $msgData = $msg->getData;
warn Dumper($msgData);
ok(ref($msgData) eq 'HASH');

my ($k,$v) = each %$msgData;
my $msg2 = Terakeet::SOAP::Message->new(
		methodName => $k,
		methodArgs => $v,
		DEBUG => 1,
		);
my $msg2String = $msg2->asString;
warn $msg2String;
ok($msg2String);
