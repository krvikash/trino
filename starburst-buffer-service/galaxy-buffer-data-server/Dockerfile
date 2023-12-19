FROM eclipse-temurin:17-jdk

ARG MODULE_NAME=galaxy-buffer-data-server
ARG PROJECT_NAME=data-server
ARG PROJECT_VERSION
ARG PROJECT_DIR="/opt/${PROJECT_NAME}"
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
ADD target/${MODULE_NAME}-${PROJECT_VERSION}.tar.gz /tmp/${PROJECT_NAME}
RUN \
    mkdir -p ${PROJECT_DIR} && \
    chown -R "${CHOWN}" ${PROJECT_DIR} && \
    mv /tmp/${PROJECT_NAME}/${MODULE_NAME}-${PROJECT_VERSION}/* ${PROJECT_DIR} && \
    chown ${CHOWN} -R ${PROJECT_DIR} && \
    rm -rf /tmp/${PROJECT_NAME} && \
    echo "${PROJECT_DIR}/bin/launcher run --config=${PROJECT_DIR}/etc/config.properties" > /start.sh && \
    chown -R "${CHOWN}" /start.sh && \
    chmod +x /start.sh

COPY --chown=${CHOWN} etc ${PROJECT_DIR}/etc
VOLUME ${PROJECT_DIR}/etc

EXPOSE 8080
USER ${CHOWN}
CMD /start.sh
