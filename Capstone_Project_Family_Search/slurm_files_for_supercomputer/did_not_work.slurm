#!/bin/bash

#SBATCH --time=72:00:00   # walltime

# 4G -> 4096M
# 8G -> 8192M
# 16G -> 16384M
# 32G -> 32768M

#SBATCH --ntasks-per-node=16   # make sure there's 16 processors per node
#SBATCH --mem-per-cpu=8192M   # memory per CPU core
#SBATCH --ntasks=32 # Number of processor cores across all nodes

#SBATCH --nodes=2 # number of nodes
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
export SPARK_PATH=/fslgroup/fslg_hadoop/spark-1.5.0/
export JAVA_HOME=/usr/java/latest/

# spark.py requires five (5) input strings: 
#       [0] CORES_PER_NODE      same as above SBATCH --ntasks-per-node
#       [1] MEM_PER_NODE        same as above SBATCH --mem-per-cpu, ***WITHOUT TRAILING LETTER, MUST BE IN MEGABYTES***
#       [2] TOTAL_EXECS         same as above SBATCH --ntasks
#       [3] entrance class name for Java or Scala program; pass an empty string when not needed for R and Python programs
#       [4] the actual program string ("program <arguments>", like you would with hadoop.py)

export SPARK_CORES_PER_NODE=16
export SPARK_MEM_PER_NODE=8192
export SPARK_TOTAL_EXECS=32

# LOAD MODULES, INSERT CODE, AND RUN YOUR PROGRAMS HERE
module load python/2/7

$SPARK_PATH/bin/spark.py $SPARK_CORES_PER_NODE $SPARK_MEM_PER_NODE $SPARK_TOTAL_EXECS "" "${HOME}/family_search/population_counter.py"

exit 0

