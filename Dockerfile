FROM willfarrell/crontab

WORKDIR /osssync/bin

ENV OSY_SOURCE_PATH "/osssync/data"
ENV OSY_CHUNK_SIZE_MB "5"
ENV OSY_OPERATION "push"
ENV OSY_FULL_INDEX "false"

ENV OSY_CONFIG_PATH ""
ENV OSY_DEST_PATH ""
ENV OSY_CREDENTIALS ""
ENV OSY_CRON ""

COPY ./osssync /osssync/bin/osssync
COPY ./startsh.sh /osssync/bin/shartsh.sh
RUN chmod 755 /osssync/bin/shartsh.sh

CMD [ "/osssync/bin/shartsh.sh" ]