#!/bin/bash

cd /home/hukumka/src/cryptostats/


if [ "stop" = "$1" ]
then
    killall collector.py &>/dev/null
    killall bot.py &>/dev/null
    exit # kill all processes and do not start any more
elif [ "git_pull" = "$1" ]
then
    echo "Pull"
    git pull origin master
    echo "Kill all"
    killall collector.py &>/dev/null
    killall bot.py &>/dev/null
    echo "Restart"
    ./collector.py -d &
    ./bot.py bot.key &
    echo "Done"
else
    ./collector.py >/dev/null &
    ./bot.py bot.key &
fi

