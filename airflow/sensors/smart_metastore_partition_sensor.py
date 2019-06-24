# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


from airflow.sensors.base_smart_operator import BaseSmartOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException, AirflowSensorException
from airflow.hooks.base_hook import BaseHook


class SmartMetastorePartitionSensor(BaseSmartOperator):
    """
    A persistent sensor service that talk directly to the Airflow metaDB
    smart-sensor will run like a service and it periodically queries
    task-instance table to find sensor tasks; poke the metastore and update
    the task instance table if it detects that certain partition or file
    created.
    """

    operator_list = ['HivePartitionSensor', 'NamedHivePartitionSensor']

    @apply_defaults
    def __init__(self,
                 *args,
                 **kwargs):
        super(SmartMetastorePartitionSensor, self).__init__(*args, **kwargs)
        self.sensor_operator = "MetastorePartitionSensor"

    def poke(self, poke_context):
        schema = poke_context.get('schema')
        table = poke_context.get('table')
        partition_name = poke_context.get('partition_name')
        conn_id = poke_context.get('conn_id')

        if '.' in table:
            schema, table = table
        sql = """
        SELECT 'X'
        FROM PARTITIONS A0
        LEFT OUTER JOIN TBLS B0 ON A0.TBL_ID = B0.TBL_ID
        LEFT OUTER JOIN DBS C0 ON B0.DB_ID = C0.DB_ID
        WHERE
            B0.TBL_NAME = '{table}' AND
            C0.NAME = '{schema}' AND
            A0.PART_NAME = '{partition_name}';
        """.format(table=table, schema=schema, partition_name=partition_name)

        conn = BaseHook.get_connection(conn_id)

        allowed_conn_type = {'google_cloud_platform', 'jdbc', 'mssql',
                             'mysql', 'oracle', 'postgres',
                             'presto', 'sqlite', 'vertica'}
        if conn.conn_type not in allowed_conn_type:
            raise AirflowException("The connection type is not supported by SqlSensor. " +
                                   "Supported connection types: {}".format(list(allowed_conn_type)))
        hook = conn.get_hook()

        self.log.info('Poking: %s (with parameters %s)', self.sql, self.parameters)
        records = hook.get_records(sql)

        return str(records[0][0]) not in ('0', '')
