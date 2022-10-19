FROM eclipse-temurin:17-jdk

ARG PROJECT_NAME=discovery-server
ARG PROJECT_VERSION
ARG CHOWN="${PROJECT_NAME}:${PROJECT_NAME}"

# Install python
RUN \
    set -xeu && \
    apt-get update -q && \
    apt-get install -q -y python2 && \
    update-alternatives --install /usr/bin/python python /usr/bin/python2 1 && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd ${PROJECT_NAME} --gid 1000 && \
    useradd ${PROJECT_NAME} --uid 1000 --gid 1000

# Add project files
ADD target/${PROJECT_NAME}-${PROJECT_VERSION}.tar.gz /tmp/${PROJECT_NAME}
RUN \
    mkdir -p /opt/${PROJECT_NAME} && \
    chown -R "${CHOWN}" /opt/${PROJECT_NAME} && \
    mv /tmp/${PROJECT_NAME}/${PROJECT_NAME}-${PROJECT_VERSION}/* /opt/${PROJECT_NAME} && \
    chown ${CHOWN} -R /opt/${PROJECT_NAME} && \
    rm -rf /tmp/${PROJECT_NAME} && \
    echo "/opt/${PROJECT_NAME}/bin/launcher run --config=/opt/${PROJECT_NAME}/etc/config.properties" > /start.sh && \
    chown -R "${CHOWN}" /start.sh && \
    chmod +x /start.sh

COPY --chown=${CHOWN} etc /opt/${PROJECT_NAME}/etc
VOLUME /opt/${PROJECT_NAME}/etc

EXPOSE 8080
USER ${CHOWN}
CMD /start.sh
