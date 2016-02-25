#!/bin/sh

if [ ! "`whoami`" = "midware" ]; then
	echo "You must run the script as 'midware'"
	exit 1
fi

current=`pwd`
base=/home/midware
cvsdir=/home/midware/midware-usbn
cd $cvsdir

#echo ""
#echo "** ant usage:"
cd $cvsdir/java
cvs update
#ant usage | grep echo
#echo "Enter any ant build arguments below."
#read antargs

date=`date "+%Y%m%d-%H%M%S"`
dist="dist-$date"
echo ""
echo "** Building distribution $date"
ant $antargs make
tar cvfz $base/$dist.tar.gz dist

echo ""
echo "** Untarring distribution"
cd $base
tar xvfz $dist.tar.gz

#echo ""
#echo "Type 'do it' to restart the middleware."
#read line
#if [ ! "$line" = "do it" ]; then
#	exit 2
#fi

echo ""
echo "** Checking for running middleware"
ps aux > $base/tmp.out
pid=`cat $base/tmp.out | grep "premier.jar" | cut -d" " -f2`
unlink $base/tmp.out

if [ ! "$pid" = "" ]; then
	echo "** Killing middleware process $pid"
	kill $pid
fi

cd $base/dist
echo ""
echo "** Starting middleware"
sh runme.sh

cd $current
