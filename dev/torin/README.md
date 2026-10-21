# Cork GPU development

How to develop cork GPU code: create an AWS EC2 GPU host and set it up, ship code to it, build, run GPU
tests and benchmarks, and remove the host. Written as a procedure that Claude (or a human) can follow step by
step.

What you get: a `g6.4xlarge` (1× NVIDIA L4 24 GB, 16 vCPU, 64 GB RAM, 300 GB gp3 root disk), running the
AWS "Deep Learning Base OSS Nvidia Driver GPU AMI (Ubuntu 26.04)", with Temurin 25, gh, `~/cork`
built, and workspace isolation set up. By default, the host is temporary and deleted when you are done with it.

The AMI already provides the NVIDIA driver, CUDA 13 and a recent git.

Use the host to run GPU code without pushing a PR: test new code while you write it, experiment, validate a
hypothesis, iterate quickly on a GPU failure, run benchmarks. Once the code is ready, CI verifies it: it runs
the GPU tests of a PR that has the `gpu` label, which the labeler adds for changes under `**/gpu/**` or
`**/torin/**` and to `**/*Gpu*` files.

## Prerequisites (local)

- `aws` CLI v2, logged in with SSO to AWS account `720124893479`. Profile names differ between people,
  so find the profile by account ID (ask the user if none matches):

  ```bash
  for p in $(aws configure list-profiles); do
      [[ "$(aws configure get sso_account_id --profile "$p")" == 720124893479 ]] && echo "$p"
  done
  ```

  Every `aws` command below assumes `export AWS_PROFILE=<profile>`. If the SSO session expired, run
  `aws sso login` (it opens a browser).
- `gh`, logged in to GitHub with access to `starburstdata/cork`.
- `~/.m2/settings.xml` with a valid CodeArtifact token (the same one used for local builds). To refresh it,
  with `AWS_PROFILE` set as above: `{ aws sts get-caller-identity || aws sso login; } && <stargate>/login.py`,
  where `<stargate>` is a checkout of [stargate](https://github.com/starburstdata/stargate), often already
  next to the main cork checkout, in `../stargate`.
- [`cloudvm`](https://github.com/findepi/cloudvm): `uv tool install --upgrade cloudvm` installs or upgrades it.

## Create the host

### 1. Choose names

Ask the user for a **resource prefix** that is unique in the shared AWS account, e.g. `john-doe`.
Do not derive it from `whoami`; local user names are not unique.

Ask whether the host should be **temporary** (the default) or **permanent**. A temporary host is deleted when
it shuts down; a permanent one is stopped instead, and can be started again later. Then:

- instance name: `<prefix>-dev-cudf-tmp-<suffix>` for a temporary host, e.g. `john-doe-dev-cudf-tmp-1`, or
  `<prefix>-dev-cudf-<suffix>` for a permanent one. Check the name is free with
  `cloudvm list --region 'eu-*,us-*' --name '<name>'`.
- key pair name: `<prefix>`; security group name: `<prefix>-sg`.

### 2. Choose the SSH key

Use an existing local key if the user has one for AWS (look in `~/.ssh/config` for an `IdentityFile`
used by their other EC2 hosts). Otherwise create one: `ssh-keygen -t ed25519 -f ~/.ssh/<prefix>-aws -N ''`.
Below, `~/.ssh/<key>` is that key's file.

### 3. Order candidate regions

Try regions nearest to the user first. Measure TCP connect time to each region's EC2 endpoint:

```bash
for r in eu-central-1 eu-central-2 eu-west-1 eu-west-2 eu-west-3 eu-north-1 eu-south-1 eu-south-2 \
         us-east-1 us-east-2 us-west-2; do
    printf '%s %s\n' "$(curl -s -o /dev/null -w '%{time_connect}' --max-time 3 "https://ec2.$r.amazonaws.com/")" "$r"
done | sort -n
```

Then go through steps 4–5 for each region in that order until a launch succeeds.

### 4. Prepare the region

```bash
export AWS_REGION=<region>
```

**Instance type offered?** `g6.4xlarge` is not available everywhere:

```bash
aws ec2 describe-instance-type-offerings --location-type availability-zone \
    --filters Name=instance-type,Values=g6.4xlarge --query 'InstanceTypeOfferings[].Location' --output text
```

Empty output: skip this region.

**AMI:**

```bash
ami=$(aws ssm get-parameter --query Parameter.Value --output text \
    --name /aws/service/deeplearning/ami/x86_64/base-oss-nvidia-driver-gpu-ubuntu-26.04/latest/ami-id)
```

**Key pair.** Key pairs are per region. Reuse it if it exists and has the same public key; import it otherwise.
This comparison works for ed25519 keys (AWS shows other fingerprints for RSA): AWS shows the same SHA256
hash as `ssh-keygen`, without the `SHA256:` prefix and with a trailing `=`.

```bash
aws ec2 describe-key-pairs --key-names <prefix> --query 'KeyPairs[].KeyFingerprint' --output text
ssh-keygen -y -f ~/.ssh/<key> | ssh-keygen -lf - -E sha256
# if missing:
aws ec2 import-key-pair --key-name <prefix> --public-key-material fileb://<(ssh-keygen -y -f ~/.ssh/<key>)
```

If a key pair with that name exists but has a different public key, stop and ask the user.

**Security group** in the default VPC, allowing SSH from the user's current public IP:

```bash
vpc=$(aws ec2 describe-vpcs --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text)
sg=$(aws ec2 describe-security-groups --filters Name=vpc-id,Values=$vpc Name=group-name,Values=<prefix>-sg \
    --query 'SecurityGroups[0].GroupId' --output text)
my_ip=$(curl -s https://checkip.amazonaws.com)
if [[ "$sg" == None ]]; then
    sg=$(aws ec2 create-security-group --vpc-id $vpc --group-name <prefix>-sg \
        --description "SSH to <prefix> dev instances" --query GroupId --output text)
fi
if ! aws ec2 describe-security-groups --group-ids $sg --output text \
        --query "SecurityGroups[0].IpPermissions[?FromPort==\`22\`].IpRanges[].CidrIp" | grep -qw "$my_ip/32"; then
    aws ec2 authorize-security-group-ingress --group-id $sg \
        --ip-permissions "IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=$my_ip/32,Description=$(hostname -s)}]"
fi
```

If there is no default VPC, stop and ask the user which VPC and public subnet to use.

### 5. Launch

```bash
aws ec2 run-instances --image-id $ami --instance-type g6.4xlarge --count 1 \
    --key-name <prefix> --security-group-ids $sg \
    --instance-initiated-shutdown-behavior terminate \
    --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=300,VolumeType=gp3,DeleteOnTermination=true}' \
    --metadata-options HttpTokens=required \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=<name>}]' \
                         'ResourceType=volume,Tags=[{Key=Name,Value=<name>}]' \
    --query 'Instances[0].[InstanceId,Placement.AvailabilityZone]' --output text
```

- Without `--subnet-id`, EC2 picks the AZ. On `InsufficientInstanceCapacity`, the error message sometimes
  lists AZs that do have capacity: retry once in each with
  `--subnet-id $(aws ec2 describe-subnets --filters Name=vpc-id,Values=$vpc Name=availability-zone,Values=<az> Name=default-for-az,Values=true --query 'Subnets[0].SubnetId' --output text)`.
  If every AZ fails, go to the next region.
- `--instance-initiated-shutdown-behavior terminate` is for a temporary host; omit it for a permanent one.
  It makes `sudo poweroff` on the host delete it. An API stop (AWS console, `aws ec2 stop-instances`) does
  **not** apply it; `cloudvm down` does (see below).
- Disk is 300 GB so that benchmark data fits later.

### 6. Connect

```bash
printf 'ubuntu\n~/.ssh/<key>\ny\n' | cloudvm up --region <region> --name <name> --update-ssh
```

It waits until the host is running and adds a `Host <name>` block to `~/.ssh/config` (backing up the old file
to `~/.ssh/config.bak`). The `printf` answers its prompts for the user, the key and the confirmation. Its own
ssh check may time out while the host boots; SSH is usually up within a minute:
`ssh -o StrictHostKeyChecking=accept-new <name> true`.

## Set up the host

From the local cork checkout:

```bash
ssh <name> mkdir -p .m2
scp ~/.m2/settings.xml <name>:.m2/
scp dev/torin/setup-host.sh <name>:/tmp/
gh auth token | ssh <name> /tmp/setup-host.sh
```

[`setup-host.sh`](setup-host.sh) installs Temurin 25 and gh. It reads a GitHub token from stdin, first thing,
and logs gh in with it. It then clones [tooltrash](https://github.com/findepi/tooltrash) into `~/bin/tools`
(for `isolate-maven-workspace`) and cork into `~/cork`. Finally it isolates `~/cork`'s Maven
workspace and runs a full `./mvnw install` without tests, which downloads all dependencies.
It is idempotent; re-run it after fixing a failure.
Expect about 15 minutes, most of it the first build; a re-run takes about 2.

On a 401 or a missing artifact from the Maven repository, the token in `settings.xml` expired: refresh it
locally (see [Prerequisites](#prerequisites-local)), copy it again with `scp ~/.m2/settings.xml <name>:.m2/`,
and re-run the script. The same applies to any later build on the host.

### Check it works

Run a couple of GPU tests:

```bash
ssh <name> 'bash -lc "cd ~/cork && ./mvnw -B -pl core/trino-main -P torin test -Dtest=TestGpuConcat,TestGpuLet -Dair.check.skip-all=true"'
```

## Working on the host

Run commands over ssh with `bash -lc '...'`. A non-interactive ssh command doesn't read `~/.profile`, where
`setup-host.sh` sets `JAVA_HOME` and `PATH`.

### Ship code with git

Work in a per-task clone, so parallel sessions don't clobber each other's checkouts and builds. Create it on
the host, from `~/cork`:

```bash
git clone ~/cork ~/cork-<task>
cd ~/cork-<task>
~/bin/tools/files/isolate-maven-workspace
git config receive.denyCurrentBranch updateInstead
```

Then push from the local checkout, and check the branch out on the first push:

```bash
git push -f <name>:cork-<task> HEAD:refs/heads/<task>   # every iteration
ssh <name> git -C cork-<task> checkout <task>           # after the first push only
```

Later pushes update the clean working tree in place (`updateInstead`).

- A clone from a local path hardlinks the objects, so it's instant and takes no extra disk.
- `~/cork` doesn't need to be up to date: the push sends only the objects the host doesn't have.
- A push carries only commits, so commit (or fixup) local edits first.

### Build and test

```bash
./mvnw -B -T1C -pl <modules>,lib/starburst-testing-auto-configuration -am -DskipTests -Dair.check.skip-all=true install
./mvnw -B -pl <module> -P torin test -Dtest='Class#m1+m2,Other' -Dair.check.skip-all=true
```

- Run the install once per clone, and again after any change in a module upstream of the one you test.
  Then run the tests without `-am`. In `-Dtest`, `,` separates classes and `+` separates methods of one class.
- The install includes `lib/starburst-testing-auto-configuration` because every module's tests depend on it,
  but `-am` doesn't build it.
- The install takes about 2 minutes, or 6–8 after a `core/trino-main` change.
- To see where a run hangs, `jstack` the test JVM; `pgrep -af surefirebooter` finds its PID. Kill it by that
  PID. In `ssh <name> '...'`, `pgrep -f`/`pkill -f` also match the remote shell running the command, so
  `pkill -f` there kills your session.

### Gotchas

- A real CUDA fault (illegal memory access, launch failure) corrupts the CUDA context, and every later test
  in the same JVM fails with `bad_alloc` or `cudaErrorLaunchFailure`. Re-run the suspect test alone.

## Benchmarks

### Benchmark workloads

The `Benchmark*` workloads in `testing/trino-tests` (`BenchmarkIcebergTpchSf30`, `BenchmarkIcebergTpcdsSf100`,
...) run TPC-H/TPC-DS queries on CPU or GPU. In a per-task clone, after an
`./mvnw -B -T1C -pl testing/trino-tests -am -DskipTests -Dair.check.skip-all=true install`:

```bash
nohup ./mvnw -B -pl testing/trino-tests test-compile exec:exec -Dair.check.skip-all=true \
    -Dexec.classpathScope=test -Dexec.executable=java \
    -Dexec.args="-cp %classpath io.trino.tests.benchmark.BenchmarkIcebergTpchSf30 run -W0 -w0 -r1 -q1 [--mode GPU]" \
    > bench.log 2>&1 &
```

- Don't add `-am`: `exec:exec` would run in every module. Re-run the install after a change outside
  `testing/trino-tests`.
- The launcher forks a child JVM sized for the workload. To run in-process instead (e.g. under a debugger),
  start `exec.args` with `-Dbenchmark.no.fork=true --add-modules=jdk.incubator.vector -Xmx<heap>`.
  For the options, see `BenchmarkRunner`.
- A workload fails fast if its dataset is stale, naming the `hydrate.sh` command to run.
- The log has `Average for qNN: ... ms` per query and `Sum of averages for all queries`. The CSV goes to
  `target/benchmark-output/<workload>/timings.csv` in the clone; CPU and GPU runs write the same file, so copy
  it before the next run.

### Data

Not downloaded during setup. The script needs AWS SSO access to the benchmark account `888469412714`. The host
has no browser, so the user logs in once, interactively. Claude copies the SSO profiles and finds the profile
name by account ID:

```bash
ssh <name> mkdir -p .aws && scp ~/.aws/config <name>:.aws/config
for p in $(aws configure list-profiles); do
    [[ "$(aws configure get sso_account_id --profile "$p")" == 888469412714 ]] && echo "$p"
done
```

Then Claude asks the user to run, with the `!` prefix, the only interactive step:

```bash
ssh -t <name> "bash -lc 'aws sso login --use-device-code --profile <profile>'"
```

The login lasts for the session lifetime (hours). `hydrate.sh` finds the profile by account ID itself.

Then, on the host, run `hydrate.sh` from up-to-date `~/cork` checkout:

```bash
git -C ~/cork pull --ff-only && ~/cork/testing/benchmark-data/hydrate.sh <dataset>...   # e.g. iceberg-tpch-sf30
```

Data goes to `~/starburst-benchmark-data/`, shared by all clones.

## Stop, start, delete

```bash
cloudvm down --region <region> --name <name> --update-ssh   # temporary: deletes it and its ssh alias; permanent: stops it
cloudvm up --region <region> --name <name> --update-ssh     # permanent: starts it again
```

`cloudvm delete --region <region> --name <name> --force` deletes any host; then remove its `Host <name>` block
from `~/.ssh/config`. To turn a temporary host into a permanent one:
`aws ec2 modify-instance-attribute --instance-id <id> --instance-initiated-shutdown-behavior Value=stop`.
