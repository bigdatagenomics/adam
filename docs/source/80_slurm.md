# Running ADAM on Slurm
 
For those groups with access to a HPC cluster managed by [Slurm](https://en.wikipedia.org/wiki/Slurm_Workload_Manager), a number of compute nodes often with a few hundred GB of local disk on each node - all attached to large shared network disk storage, it is possible to spin up a temporary Spark cluster for use by ADAM.  
 
While the full IO bandwidth benefits of Spark processing are likely best realized through a set of co-located compute/storage nodes, depending of on your network setup you may find Spark deployed on HPC to be a workable solution for testing or even production at scale, especially for those applications which perform multiple in-memory transformations and thus benefit from Spark's in-memory processing model.
 
Follow the primary instructions in the ADAM README.md for installing ADAM into `$ADAM_HOME` 
 
## Start Spark cluster 
 
A Spark cluster can be started as a muti-node job in Slurm by creating a job file `run.cmd` such as below:
```
#!/bin/bash
 
#SBATCH --partition=multinode
#SBATCH --job-name=spark-multi-node
#SBATCH --exclusive
 
#Number of seperate nodes reserved for Spark cluster
#SBATCH --nodes=2
#SBATCH --cpus-per-task=12
 
#Number of excecution slots
#SBATCH --ntasks=2
 
#SBATCH --time=05:00:00
#SBATCH --mem=248g
 
# If your sys admin has installed spark as a module
module load spark
 
# If spark is not installed as a module, you will need to specifiy absolute path to $SPARK_HOME/bin/spark-start
start-spark
 
echo $MASTER
sleep infinity
```
submit the job file to Slurm:
```
sbatch run.cmd
```
 
This will start a Spark cluster containing 2 nodes that persists for 5 hours, unless you kill it sooner.
The `slurm.out` file  created in the current directory will contain a line produced by `echo $MASTER` above which will 
indicate the address of the Spark master to which your application or ADAM-shell should connect such as `spark://somehostname:7077`
 
## Start ADAM Shell
Your sys admin will probably prefer that you aunch your ADAM-shell or start an application from a cluster node rather than the head node you log in to so you may want to do so with:
```
sinteractive 
```
 
Start an adam-shell as so:
```
$ADAM_HOME/bin/adam-shell --master spark://hostnamefromslurmdotout:7077
```
 
## Or run ADAM submit
```
$ADAM_HOME/bin/adam-submit --master spark://hostnamefromslurmdotout:7077
```
 
You should be able to connect to the Spark Web UI at `spark://hostnamefromslurmdotout:4040`, however you may need to ask your local sys admin to open the requried ports.
 
## Feedback
We'd love to hear feedback on your experience running ADAM on HPC/Slurm or other deployment architectures, and let us know of any problems you run into via the mailing list or Gitter.
