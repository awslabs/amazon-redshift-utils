import sys
import datetime

'''
aws_utils.py

Utility modules for the amazon-redshift-utils family of tools

Ian Meyers
Amazon Web Services (2017)
'''

debug = False

# emit a single cloudwatch metric with the given dimensions
def put_metric(cw, namespace, metric_name, dimensions, timestamp, value, unit):
    emit_metrics(cw, namespace, [{
        'MetricName': metric_name,
        'Dimensions': dimensions,
        'Timestamp': datetime.datetime.utcnow() if timestamp is None else timestamp,
        'Value': value,
        'Unit': unit
    }])

# emit all the provided cloudwatch metrics consistent with API limits around batching
def emit_metrics(cw, namespace, put_metrics):
    max_metrics = 20
    group = 0
    print("Publishing %s CloudWatch Metrics" % (len(put_metrics)))

    for x in range(0, len(put_metrics), max_metrics):
        group += 1

        # slice the metrics into blocks of 20 or just the remaining metrics
        put = put_metrics[x:(x + max_metrics)]

        if debug:
            print("Metrics group %s: %s Datapoints" % (group, len(put)))
            print(put)
        try:
            cw.put_metric_data(
                Namespace=namespace,
                MetricData=put
            )
        except:
            print('Pushing metrics to CloudWatch failed: exception %s' % sys.exc_info()[1])
