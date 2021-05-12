set -e
bucket_name=""
bucket_keyprefix=$1
start_time=$2
end_time=$3
#
echo "bucket_name: $bucket_name"
echo "bucket_keyprefix: $bucket_keyprefix"
echo "start_time: $start_time"
echo "end_time: $end_time"
#
# configure extraction.yaml configuration file
#
cd /amazonutils/amazon-redshift-utils/src/SimpleReplay
mkdir -p $bucket_keyprefix
aws s3 cp s3://$bucket_name/config/extract.yaml ./$bucket_keyprefix/extract.yaml
sed -i "s#mybucketname/myworkload#$bucket_name/$bucket_keyprefix/extract#g" ./$bucket_keyprefix/extract.yaml
sed -i "s#start_time: \"\"#start_time: \"$start_time\"#g" ./$bucket_keyprefix/extract.yaml
sed -i "s#end_time: \"\"#end_time: \"$end_time\"#g" ./$bucket_keyprefix/extract.yaml
sed -i "s#source_cluster_system_table_unload_location: \"\"#source_cluster_system_table_unload_location: \"s3://$bucket_name/$bucket_keyprefix/source_system_tables\"#g" ./$bucket_keyprefix/extract.yaml
#
# run extract process
#
python3 extract.py ./$bucket_keyprefix/extract.yaml
#
# upload metadata
#
output=$(aws s3 ls s3://$bucket_name/$bucket_keyprefix/extract/ | awk '{print $2}')
extract_output=${output::-1}
echo "{\"prefix\": \"$bucket_keyprefix\", \"extract_output\": \"$extract_output\"}" > extract_prefix.json
aws s3 cp extract_prefix.json s3://${bucket_name}/config/
