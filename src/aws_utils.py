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

def set_search_paths(conn, schema_name, set_target_schema=None, exclude_external_schemas=False):
    get_schemas_statement = '''
        select nspname
        from pg_catalog.pg_namespace
        where nspname ~ '%s'
    ''' % schema_name

    if exclude_external_schemas is True:
        get_schemas_statement += " and nspname not in (select schemaname from svv_external_schemas)"

    # set default search path
    search_path = 'set search_path = \'$user\',public'

    # add the target schema to the search path
    if set_target_schema is not None and set_target_schema != schema_name:
        search_path = search_path + ', %s' % set_target_schema

    # add all matched schemas to the search path - this could be a single schema, or a pattern
    c = conn.cursor()
    c.execute(get_schemas_statement)
    results = c.fetchall()

    for r in results:
        search_path = search_path + ', %s' % r[0]

    if debug:
        print(search_path)

    c = conn.cursor()
    c.execute(search_path)
