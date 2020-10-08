#!/bin/sh

# usage:   cleohome=$(servicehome $service)
# returns: the CLEOHOME parsed from the $service conf file
servicehome() {
    local init service
    init=`ps -p1 | grep systemd > /dev/null && echo systemd || echo upstart`
    service=$1
    if [ "$init" = "systemd" ]; then
        sed -n '/^Environment=CLEOHOME=/s/.*=\s*//p' /etc/systemd/system/$service.service 2>/dev/null
    else
        sed -n '/^env\s*CLEOHOME=/s/.*=\s*//p' /etc/init/$service.conf 2>/dev/null
    fi
}

# usage:   cleohome=$(findhome arg)
# returns: the (suspected) path to Harmony/VLTrader's install directory based on the argument
findhome() {
    local cleohome arg
    arg=$1
    cleohome=$(servicehome $arg)
    if [ "$cleohome" = "" ]; then
        if [ -e "$arg/Harmonyc" -o -e "$arg/VLTraderc"  ]; then
            cleohome=$arg
        fi
    fi
    echo $cleohome
}

here=$(cd `dirname $0` && pwd -P)
cleohome=$(findhome $1)
if [ "$cleohome" != "" ]; then
    shift;
elif [ "$1" != "" -a -d "$1" ]; then
    # assume this was supposed to be a CLEOHOME
    :
elif [ -e "./Harmonyc" -o -e "./VLTraderc" ]; then
    cleohome=.
else
    cleohome=$(servicehome cleo-harmony)
fi
if [ "$cleohome" != "" ]; then
    cleohome=$(cd $cleohome && pwd -P)
    unset DISPLAY
    classpath=$HOME/.cleo/cache/connector-batchapi-0.9-RC2-SNAPSHOT-commandline.jar:$(find $cleohome/lib -type d|sed 's|$|/*|'|paste -s -d : -):$cleohome/webserver/AjaxSwing/lib/ajaxswing.jar
    (CLEOHOME=$cleohome $cleohome/jre/bin/java -cp $classpath com.cleo.labs.connector.batchapi.processor.Main "$@")
else
    echo "Cleo installation not found"
fi
