#!/bin/sh

if [ ! "`whoami`" = "midware" ]; then
	echo "You must run the script as 'midware'"
	exit 1
fi

dev="/dev"
live=""
if [ "$1" = "live" ]; then
	dev=""
	live="live"
	echo "** LIVE"
else
	echo "** DEVELOPMENT"
fi

current=`pwd`
home=/home/midware
base=/home/midware$dev
cvsdir=/home/midware$dev/midware-usbn
cd $cvsdir/java
echo "** Updating from CVS"
cvs update

date=`date "+%Y%m%d-%H%M%S"`
dist="dist-$date"
if [ "$dev" = "/dev" ]; then
	dist="dist-dev-$date"
fi

echo ""
echo "** Building distribution $date"
ant $antargs make

if [ "$dev" = "/dev" ]; then
	echo "** Renaming development jar"
	mv dist/premier.jar dist/premier-dev.jar
fi

echo "** Packaging distribution $date"
tar cvfz $base/$dist.tar.gz dist

echo ""
echo "** Untarring distribution"
cd $base
tar xvfz $dist.tar.gz

echo ""
echo "** Archiving distribution"
mv $dist.tar.gz dist-archive/.

echo ""
echo "PRESS [ENTER] TO RESTART *$mode* MIDDLEWARE"
read enter

sh $home/restart.sh $live

cd $current
