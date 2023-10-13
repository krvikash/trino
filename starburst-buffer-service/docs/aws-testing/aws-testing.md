Below is a raw description of how to setup an AWS environment for testing Trino with buffer service.

## prerequisites

* Access to `888469412714` AWS account
* `eksctl` installed
* `kubectl` installed

## login to AWS account

For purpose of testing we are using `engineering` AWS account.
Login using

```bash
GIMME_AWS_CREDS_CRED_PROFILE=default gimme-aws-creds --roles '/888469412714/'
```

For reference my `default` profile from `~/.okta_aws_login_config`:
```
[DEFAULT]
okta_org_url = https://starburstdata.okta.com
okta_auth_server =
client_id =
gimme_creds_server = appurl
aws_appname =
aws_rolename = arn:aws:iam::888469412714:role/engineering
write_aws_creds = True
cred_profile = default
okta_username = lukasz.osipiuk@starburstdata.com
app_url = https://starburstdata.okta.com/home/amazon_aws/0oa1esd8q3cuIvBY1357/272
resolve_aws_alias = True
include_path = True
preferred_mfa_type = push
remember_device = True
aws_default_duration = 14400
device_token =
output_format =
```

## create AWS cluster

### pick a name

```shell
CLUSTER_NAME=my_test_cluster
```

### create cluster
```shell
eksctl create cluster \
 --name ${CLUSTER_NAME} \
 --region us-east-1 \
 --vpc-public-subnets subnet-00d038b895957d5fa,subnet-094d5ef825f31cc6c,subnet-00ca6702535c8fad9 \
 --without-nodegroup
 
eksctl utils update-kube-proxy \
 --cluster ${CLUSTER_NAME} \
 --approve
```

### create node groups
```shell
NODES_COUNT=2

eksctl create nodegroup \
 --region us-east-1 \
 --cluster tardigrade-testing-lo \
 --name trino-ng \
 --node-type m6g.8xlarge \
 --subnet-ids subnet-00d038b895957d5fa \
 --node-security-groups sg-0e015e83e991d4414 \
 --nodes ${NODES_COUNT}

eksctl create nodegroup \
 --region us-east-1 \
 --cluster tardigrade-testing-lo \
 --name bs-ng \
 --node-type m6g.2xlarge \
 --subnet-ids subnet-00d038b895957d5fa \
 --node-security-groups sg-0e015e83e991d4414 \
 --nodes ${NODES_COUNT}
```

## build artifacts

### prepare build environment

```shell
ECR_PREFIX=888469412714.dkr.ecr.us-east-1.amazonaws.com/test-losipiuk
TRINO_ECR=${ECR_PREFIX}/trino
BS_ECR_PREFIX=${ECR_PREFIX}/trino-buffer-service
BS_DATA_SERVER_ECR=${BS_ECR_PREFIX}/data-server
BS_DISCOVERY_SERVER_ECR=${BS_ECR_PREFIX}/discovery-server
BS_ARCH=linux/arm64
TRINO_ECR=${ECR_PREFIX}/trino

TRINO_DIR=~/workspace/repos/github.com/trinodb/trino
BS_DIR=~/workspace/repos/github.com/starburstdata/trino-buffer-service
STARGATE_DIR=~/workspace/repos/github.com/starburstdata/stargate
TRINO_CHARTS_DIR=~/Users/lukaszos/workspace/repos/github.com/trinodb/charts
```

Note: adjust paths to match your local env

### login to ecr

```shell
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 888469412714.dkr.ecr.us-east-1.amazonaws.com
```

### pick an image tag

```shell
TEST_RELEASE_TAG="lo-19.09.23-7"
```

## build buffer service images

```shell
cd ${BS_DIR}
(
set -e
mvnd clean install -TC2 -DskipTests

./bin/build-docker-image.sh -p discovery-server -r ${ECR_PREFIX} -v 1-SNAPSHOT -a ${BS_ARCH}
DISCOVERY_SERVER_IMAGE="${BS_DISCOVERY_SERVER_ECR}:${TEST_RELEASE_TAG}"
echo "discovery-server image: ${DISCOVERY_SERVER_IMAGE}"
docker tag ${BS_DISCOVERY_SERVER_ECR}:1-SNAPSHOT ${DISCOVERY_SERVER_IMAGE}
docker push ${DISCOVERY_SERVER_IMAGE}

./bin/build-docker-image.sh -p data-server -r ${ECR_PREFIX} -v 1-SNAPSHOT -a ${BS_ARCH}
DATA_SERVER_IMAGE="${BS_DATA_SERVER_ECR}:${TEST_RELEASE_TAG}"
echo "data-server image: ${DATA_SERVER_IMAGE}"
docker tag ${BS_DATA_SERVER_ECR}:1-SNAPSHOT ${DATA_SERVER_IMAGE}
docker push ${DATA_SERVER_IMAGE}
) \
&& git tag $TEST_RELEASE_TAG
```

## setup OSS Trino to include buffer service exchange

First checkout commit you care about.
Then apply `add-bs-exchange.diff` patch
```shell
CUR_DIR=`pwd`
(
  cd $TRINO_DIR
  patch -p1 < $CUR_DIR/add-bs-exchange.diff
)
```

Optionally appy patch which removes some of unneeded plugins to make Trino image smaller
```shell
CUR_DIR=`pwd`
(
  cd $TRINO_DIR
  patch -p1 < $CUR_DIR/remove-plugins.diff
)
```

Probably you want to commit the changes locally to have a clean working copy

## build Trino image

```shell
cd  ${TRINO_DIR}
(
set -e
mvnd clean install -T 1C -DskipTests -Dmaven.javadoc.skip=true -Dmaven.source.skip=true -Dair.check.skip-all=true
./core/docker/build.sh -a arm64
TRINO_VERSION=$(cd ${TRINO_DIR} && ./mvnw -B help:evaluate -Dexpression=pom.version -q -DforceStdout)
SOURCE_TRINO_IMAGE=trino:${TRINO_VERSION}-arm64
TARGET_TRINO_IMAGE=${TRINO_ECR}:"${TEST_RELEASE_TAG}"
echo "trino image: ${TARGET_TRINO_IMAGE}"
docker tag ${SOURCE_TRINO_IMAGE} ${TARGET_TRINO_IMAGE}
docker push ${TARGET_TRINO_IMAGE}
) \
&& git tag $TEST_RELEASE_TAG
```

# deploy services

## deploy buffer service

Copy `buffer-service-values-example.yaml` to `buffer-service-values.yaml` and make needed changes; make sure you set 
the number of buffer nodes in the yaml.

Install buffer service helmchart:

```shell
helm upgrade --install --values buffer-service-values.yaml tardigrade-testing-bs $STARGATE_DIR/charts/trino-buffer-service
```

## deploy Trino

Copy `trino-values-example.yaml` to `trino-values.yaml` and make needed changes; make sure you set the number 
of workers in the yaml.

Install Trino helmchart:
```shell
helm upgrade --install --values trino-values.yaml tardigrade-testing-batch $TRINO_CHARTS_DIR/charts/trino
```

## setup Trino port forwarding

```shell
kubectl port-forward $(kubectl get pods --namespace default -l "app=trino,release=tardigrade-testing-batch,component=coordinator" -o jsonpath="{.items[0].metadata.name}") 8080:8080 5005:5005
```

# Use the environment

## Use CLI

```shell
$TRINO_DIR/client/trino-cli/target/trino-cli-*-executable.jar --server localhost:8080 --catalog hive --schema tpcds_sf1000_partitioned
```

# cleanup

## uninstall Trino

```shell
helm uninstall tardigrade-testing-batch
```

## uninstall Buffer service

```shell
helm uninstall tardigrade-testing-bs
```

## Scale node cluster node groups to 0

```shell
eksctl scale nodegroup --region us-east-1 --cluster ${CLUSTER_NAME} --name $(eksctl get nodegroups --region us-east-1 --cluster ${CLUSTER_NAME} --output json |jq -r .[0].Name) --nodes-min 0 --nodes-max 1 --nodes 0
```

## Delete cluster

We typically do not delete cluster as it takes a lot of time to spin it up. But if you have to then below snippet  should work:

```shell
eksctl delete cluster \
 --name ${CLUSTER_NAME} \
 --region us-east-1 
```

# TODO
 * describe how to setup mysql for event listener
 * how to use verifier

