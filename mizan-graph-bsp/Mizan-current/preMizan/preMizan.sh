if [ $# -ne 2 ]
then
        echo "Command error, input format = $0 [input graph] [cluster/partition size]"
        exit -1
fi


inputFile=$(basename $1)

hadoop dfsadmin -safemode wait
hadoop dfs -rm input/$inputFile
hadoop dfs -put $1 input/$inputFile

echo "Select your partitioning type:"
    echo "   1) Hash Graph Partitioning"
    echo "   2) Range Graph Partitioning"
while true; do
    read -p "" yn
    case $yn in
        [1]* ) cd hadoopScripts; ./hadoop_run_modhash.sh $inputFile $2 true; break;;
        [2]* ) cd hadoopScripts; ./hadoop_run_range.sh $inputFile $2 true; break;;
        * ) echo "Please answer by typing 1 or 2.";;
    esac
done
