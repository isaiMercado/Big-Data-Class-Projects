#!/bin/bash

#SBATCH --time=72:00:00   # walltime

# 4G -> 4096M
# 8G -> 8192M
# 16G -> 16384M
# 32G -> 32768M

#SBATCH --ntasks-per-node=5   # make sure there's 16 processors per node
#SBATCH --mem-per-cpu=4096M   # memory per CPU core
#SBATCH --ntasks=25 # Number of processor cores across all nodes

#SBATCH --nodes=5 # number of nodes
#SBATCH -J "Family_Search"   # job name

#SBATCH --mail-user=isai.mercado.oliveros@gmail.com   # email address
#SBATCH --mail-type=BEGIN
#SBATCH --mail-type=END
#SBATCH --mail-type=FAIL

# Compatibility variables for PBS. Delete if not needed.
export PBS_NODEFILE=`/fslapps/fslutils/generate_pbs_nodefile`

# These variables are set to shorten the command below, but are also critical for correct behavior.
# If you are trying to use a different value for JAVA_HOME, ***CHANGING IT HERE MAY NOT SUFFICE***

export HADOOP_GROUP=/fslgroup/fslg_hadoop/
export SPARK_PATH=/fslgroup/fslg_hadoop/spark-1.5.0/bin
export JAVA_HOME=/usr/java/latest/

module load python/2/7

$SPARK_PATH/pyspark "${HOME}/family_search/population_counter.py"

exit 0

