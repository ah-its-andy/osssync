#!/bin/bash

if [ -z "$OSY_CRON" ]; then 
    sh -c '/osssync/bin/osssync'    
else
    if [ -z "$OSY_EXEC_NOW" ];then
        echo "Execute osssync in cron"
    else 
        echo 'executing now'
        sh -c '/osssync/bin/osssync'
    fi
    echo "${OSY_CRON} /osssync/bin/osssync > /dev/stdout" >> /etc/crontabs/root
    sh -c 'cat /etc/crontabs/root'
    sh -c 'crond  -f -d 6 -c /etc/crontabs'
fi