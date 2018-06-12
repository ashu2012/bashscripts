for i in `cat filenames.txt |   sed 's/\r$//'` ; 
do
	#i= ${i%$'\r\n'}
	echo "$i"

    
	#f = readwholeWhile
	#f=`cat $file`
	#print $f
	
	#f="show databases;"
	
read -r -d ''	query<< EOM
use dsp_aero_wheelsbrakes_sbx;

	with boundaries as 
(
select fileindex as end_index ,lag_fileindex as start_index
from (
select filename,fileindex,status,lag(fileindex,400,0) over (partition by filename order by fileindex) as lag_fileindex
from test_table12 where filename = "$i"
) q
where status=1
)
insert into table takeoff_data
select a.fileindex,a.filename,a.groundsped_adiru,a.agdlkd_pseu1,a.status,Concat(a.filename, '-', b.start_index) AS takeoff_id
from (select * from test_table12 where filename = "$i") a, boundaries b
where a.fileindex>=b.start_index and a.fileindex<=end_index order by a.fileindex;
EOM

	echo "$query"

	beeline -u "jdbc:hive2://zk0-sal04s.sentience.local:2181,zk1-sal04s.sentience.local:2181,zk2-sal04s.sentience.local:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"  --silent=TRUE --outputformat=csv2 -e "$query"
	echo --------------------------------------------------------$'\n'
#break

done;
