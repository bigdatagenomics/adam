# Deploying ADAM

## Running ADAM on EC2

First, export some variables for running EC2:

```bash
export AWS_ACCESS_KEY_ID='?????'
export AWS_SECRET_ACCESS_KEY='?????'
export MY_KEYPAIR="?????"      # your keypair in us-east
export MY_KEYFILE="?????.pem"
export MY_CLUSTER_NAME="adam_cluster"
export MY_CLUSTER_SIZE=10

# M2 and CR1 are memory optimized
export MY_INSTANCE_TYPE="m2.4xlarge"
```

If you want to use spot pricing, add `--spot-price` as an option to
`spark_ec2_launch` (below) and `export MY_SPOT_PRICE=1.399`.

Export the path to your `spark-ec2` script,

```bash
export SPARK_EC2_SCRIPT="/path/to/spark-0.8.1/ec2/spark-ec2"   # CHANGE ME
```

Set up some aliases for commands to the spark ec2 script,

```bash
alias spark_ec2_launch="$SPARK_EC2_SCRIPT -k $MY_KEYPAIR \
-i $MY_KEYFILE -s $MY_CLUSTER_SIZE --zone us-east-1c \
--instance-type=$MY_INSTANCE_TYPE launch $MY_CLUSTER_NAME"
alias spark_ec2_stop="$SPARK_EC2_SCRIPT stop $MY_CLUSTER_NAME"
alias spark_ec2_start="$SPARK_EC2_SCRIPT -i $MY_KEYFILE start $MY_CLUSTER_NAME"
alias spark_ec2_destroy="$SPARK_EC2_SCRIPT destroy $MY_CLUSTER_NAME"
alias spark_ec2_login="$SPARK_EC2_SCRIPT -k $MY_KEYPAIR -i $MY_KEYFILE login $MY_CLUSTER_NAME"
```

Now you can run:
* `spark_ec2_launch` to launch your cluster,
* `spark_ec2_stop` to stop the cluster (your data is not deleted),
* `spark_ec2_start` to restart your cluster,
* `spark_ec2_destroy` to stop the cluster and cleanup all data,
* `spark_ec2_login` to log into the master node of your cluster.

Launching a cluster takes about 10 minutes. When the spark ec2 script finishes,
it will give you the location of your spark master web UI and ganglia UI. You
may want to open both URLs in tabs and 'pin' them to return to later.

Once you have the cluster running, you will need to scp `adam-x.y.jar` to the
master node, e.g.
`scp -i /path/to/key.pem adam-x.y.jar root@ec2-107-21-175-59.compute-1.amazonaws.com:`
(don't forget the *colon* at the end).

## Running ADAM on CDH 5 and other YARN based Distros

## Running ADAM on Toil