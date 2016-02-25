# test script for Terakeet::SOAP
# $Id: 02request.t,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $

use Test;
BEGIN { plan tests=>1 };

use Terakeet::SOAP::Client;

eval { require("t/settings.req"); } ||
eval { require("settings.req"); };

my $client = Terakeet::SOAP::Client->new(
		Host	=> $TEST_HOST,
		Port	=> $TEST_PORT,
		SSL	=> $TEST_SSL,
		Wait	=> $TEST_WAIT,
		Debug	=> $TEST_DEBUG,
		);
unless ($client->connect) {
	print "Unable to connect to a host.  You'll have to edit settings.req",
	      " in the t/ directory and put in some legitimate values.";
	exit;
}

my $response = $client->call_method($TEST_METHOD, %TEST_PARAMS);
$client->disconnect;
my $resp = $response->asString;
warn $resp;
ok($resp);
