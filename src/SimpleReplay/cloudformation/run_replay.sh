set -e
extract_bucket=""
replay_bucket=""
copy_replacements=""
redshift_iam_role=""
bucket_keyprefix=$1
extract_output=$2
replay_type=$3
cluster_endpoint=$4
#
echo "extract_bucket: $extract_bucket"
echo "replay_bucket: $replay_bucket"
echo "copy_replacements: $replay_bucket"
echo "bucket_keyprefix: $bucket_keyprefix"
echo "extract_output: $extract_output"
echo "replay_type: $replay_type"
echo "cluster_endpoint: $cluster_endpoint"
#
# configure replay.yaml configuration file for target and replica clusters
#
cd /amazonutils/amazon-redshift-utils/src/SimpleReplay
mkdir -p $bucket_keyprefix
aws s3 cp s3://$replay_bucket/config/replay.yaml ./$bucket_keyprefix/replay_target.yaml
sed -i "s#workload_location: \"\"#workload_location: \"s3://$extract_bucket/$bucket_keyprefix/extract/$extract_output\"#g" ./$bucket_keyprefix/replay_target.yaml
sed -i "s#target_cluster_endpoint: \"\"#target_cluster_endpoint: \"$cluster_endpoint\"#g"  ./$bucket_keyprefix/replay_target.yaml
#
cp -f ./$bucket_keyprefix/replay_target.yaml ./$bucket_keyprefix/replay_replica.yaml
sed -i "s#replay_output: \"\"#replay_output: \"s3://$replay_bucket/$bucket_keyprefix/replay/replay_output_target\"#g" ./$bucket_keyprefix/replay_target.yaml
sed -i "s#replay_output: \"\"#replay_output: \"s3://$replay_bucket/$bucket_keyprefix/replay/replay_output_replica\"#g" ./$bucket_keyprefix/replay_replica.yaml
#
aws s3 cp ./$bucket_keyprefix/replay_target.yaml s3://$replay_bucket/$bucket_keyprefix/replay/
aws s3 cp ./$bucket_keyprefix/replay_replica.yaml s3://$replay_bucket/$bucket_keyprefix/replay/
aws s3 cp s3://$extract_bucket/$bucket_keyprefix/source_system_tables/ s3://$replay_bucket/$bucket_keyprefix/source/detailed_query_stats/ --recursive
#
if [[ $copy_replacements == "true" ]]; then
 aws s3 cp s3://$extract_bucket/$bucket_keyprefix/extract/$extract_output/copy_replacements.csv . || true
 sed -i "s#,,#,,$redshift_iam_role#g" copy_replacements.csv || true
 aws s3 cp copy_replacements.csv s3://$extract_bucket/$bucket_keyprefix/extract/$extract_output/copy_replacements.csv || true
fi
python3 replay.py ./$bucket_keyprefix/replay_$replay_type.yaml
