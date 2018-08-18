#!/bin/bash

cd /home/hukumka/src/cryptostats/

killall collector.py &>/dev/null
killall discord_bot.py &>/dev/null

if [ "stop" = "$1" ]
then
    exit # kill all processes and do not start any more
elif [ "git_pull" = "$1" ]
then
    git pull origin master
    ./collector.py -d -v &
else
    ./collector.py -v &
fi

python3 discord_bot.py &
