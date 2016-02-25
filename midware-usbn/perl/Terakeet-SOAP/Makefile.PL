# $Id: Makefile.PL,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $

use ExtUtils::MakeMaker;

WriteMakefile(
	'NAME'		=> 'Terakeet::SOAP',
	'VERSION_FROM'	=> 'lib/Terakeet/SOAP.pm',
	'PREREQ_PM'	=> {
				'XML::DOM' => '1.43',
				'IO::Socket' => '1.18',
				'IO::Socket::SSL' => '0.95',
			   },
	'dist'		=> { 'COMPRESS' => 'gzip', 'SUFFIX' => '.gz' },
);
