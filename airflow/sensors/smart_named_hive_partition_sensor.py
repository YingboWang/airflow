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


"""
    Query Airflow DB for all


"""
from airflow.sensors.base_smart_operator import BaseSmartOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from airflow.utils.decorators import apply_defaults


class SmartNamedHivePartitionSensor(BaseSmartOperator):
    """
    A persistent sensor service that talk directly to the Airflow metaDB
    smart-sensor will run like a service and it periodically queries
    task-instance table to find sensor tasks; poke the metastore and update
    the task instance table if it detects that certain partition or file
    created.


    """

    operator_type = NamedHivePartitionSensor

    @apply_defaults
    def __init__(self,
                 # task_id,
                 *args,
                 **kwargs):
        # self.task_id = task_id

        super(SmartNamedHivePartitionSensor, self).__init__(*args, **kwargs)
        self.sensor_operator = "NamedHivePartitionSensor"
        self.persist_fields = NamedHivePartitionSensor.persist_fields

    @staticmethod
    def parse_partition_name(partition):
        first_split = partition.split('.', 1)
        if len(first_split) == 1:
            schema = 'default'
            table_partition = max(first_split)  # poor man first
        else:
            schema, table_partition = first_split
        second_split = table_partition.split('/', 1)
        if len(second_split) == 1:
            raise ValueError('Could not parse ' + partition +
                             'into table, partition')
        else:
            table, partition = second_split
        return schema, table, partition

    def poke_partition(self, hook, partition, conn_id):
        if not hook:
            from airflow.hooks.hive_hooks import HiveMetastoreHook
            hook = HiveMetastoreHook(
                metastore_conn_id=conn_id)

        schema, table, partition = self.parse_partition_name(partition)

        self.log.info('Poking for %s.%s/%s', schema, table, partition)
        return hook.check_for_named_partition(
            schema, table, partition)

    def poke(self, poke_context):
        metastore_conn_id = poke_context.get('metastore_conn_id')
        hook = poke_context.get('hook', None)

        partition_names = [
            partition_name for partition_name in poke_context['partition_names']
            if not self.poke_partition(hook, partition_name, metastore_conn_id)
        ]
        return not partition_names

