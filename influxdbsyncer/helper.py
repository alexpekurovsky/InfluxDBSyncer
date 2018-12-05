import pandas as pd
import numpy as np
import argparse
import ConfigParser
import sys

def load_config():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", dest='config_file', default='/etc/influxdbsyncer/influxdbsyncer.conf')
    parser.add_argument("--local-host", dest='local_host', default=None)
    parser.add_argument("--local-port", dest='local_port', default=None)
    parser.add_argument("--local-db", dest='local_db', default=None)
    parser.add_argument("--local-user", dest='local_user', default=None)
    parser.add_argument("--local-pass", dest='local_pass', default=None)
    parser.add_argument("--remote-host", dest='remote_host', default=None)
    parser.add_argument("--remote-port", dest='remote_port', default=None)
    parser.add_argument("--remote-db", dest='remote_db', default=None)
    parser.add_argument("--remote-user", dest='remote_user', default=None)
    parser.add_argument("--remote-pass", dest='remote_pass', default=None)
    parser.add_argument("--skip-last", dest='skip_last', default=None)
    parser.add_argument("--sync-window", dest='sync_window', default=None)
    parser.add_argument("--every", dest='every', default=None)
    parser.add_argument("--batch-size", dest='batch_size', default=None)
    args = parser.parse_args()
    config = ConfigParser.ConfigParser()
    config.read(args.config_file)
    merge_config_args(config, 'general', 'skip_last',   args.skip_last)
    merge_config_args(config, 'general', 'sync_window', args.sync_window)
    merge_config_args(config, 'general', 'every',       args.every)
    merge_config_args(config, 'general', 'batch_size',  args.batch_size)
    merge_config_args(config, 'local',   'host',        args.local_host)
    merge_config_args(config, 'local',   'port',        args.local_port)
    merge_config_args(config, 'local',   'db',          args.local_db)
    merge_config_args(config, 'local',   'user',        args.local_user)
    merge_config_args(config, 'local',   'pass',        args.local_pass)
    merge_config_args(config, 'remote',  'host',        args.remote_host)
    merge_config_args(config, 'remote',  'port',        args.remote_port)
    merge_config_args(config, 'remote',  'db',          args.remote_db)
    merge_config_args(config, 'remote',  'user',        args.remote_user)
    merge_config_args(config, 'remote',  'pass',        args.remote_pass)
    return config

def merge_config_args(config, section, variable, value):
    if value is not None:
        config.set(section, variable, value)

def get_measurements(client):
    measurements = []
    results = client.query('SHOW MEASUREMENTS')
    for measurement in results.get_points():
        measurements.append(measurement['name'])
    return measurements

def get_tag_keys(client, measurement):
    query = 'SHOW TAG KEYS FROM "%s"' % measurement
    results = client.query(query)
    tag_keys = []
    for tag in results[measurement]:
        tag_keys.append(tag['tagKey'])
    return tag_keys

def get_field_keys(client, measurement):
    query = 'SHOW FIELD KEYS FROM "%s"' % measurement
    results = client.query(query)
    field_keys = {}
    for field in results[measurement]:
        field_keys[field['fieldKey']] = field['fieldType']
    return field_keys

def get_points(client, measurement, start_date, end_date):
    query = "SELECT * FROM %s WHERE time >= '%s' AND time <= '%s'" % (measurement, start_date.isoformat() + 'Z', end_date.isoformat() + 'Z')
    results = client.query(query)
    if results.has_key(measurement):
        result = results[measurement]
        result.index.name = 'time'
        result.index = result.index.floor('s')
        return result
    else:
        return pd.DataFrame()

def get_deltas(remote_points, local_points, tag_keys):
    merge_on = ['time'] + tag_keys
    merged = remote_points.merge(local_points, suffixes=["_remote", "_local"], on=merge_on, how='outer')
    local_columns = []
    remote_columns = []
    for column in merged.columns.values:
        if column.endswith('_local'):
            local_columns.append(column)
        if column.endswith('_remote'):
            remote_columns.append(column)
    for local_column in local_columns:
        if np.issubdtype(merged[local_column], np.number):
            merged = merged[np.isnan(merged[local_column])]
    for local_column in local_columns:
        merged[local_column] = merged[local_column.replace("_local", "_remote")]
    for remote_column in remote_columns:
        del merged[remote_column]
    renamer = {}
    for local_column in local_columns:
        renamer[local_column] = local_column.replace("_local", "")
    merged = merged.rename(columns = renamer)
    return merged

def write_data(client, measurement, data, tag_keys, field_keys, batch_size):
    field_columns = field_keys.keys()
    for field_name in field_keys.keys():
        if field_keys[field_name] != 'string':
            data[field_name] = pd.to_numeric(data[field_name], downcast=field_keys[field_name])
    client.write_points(data, measurement, tag_columns=tag_keys, field_columns=field_columns, protocol='line', batch_size=batch_size)
