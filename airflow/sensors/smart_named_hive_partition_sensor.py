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
from airflow.models.skipmixin import SkipMixin
from airflow import models
from airflow.utils.db import provide_session
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from airflow.utils import timezone
from airflow.exceptions import AirflowException, AirflowSensorException
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import (Column, Index, Integer, String, and_, func, not_, or_)
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm.session import make_transient
import yaml

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
        self.poke_dict = {}
        self.task_dict = {}
        self.failed_dict = {}

    @provide_session
    def init_poke_dict(self, session=None):
        """
        Initiate poke dict based on operators. Need to improve for sharding big amount.
        :param operators:
        :return:
        """
        # ============Include testing default info. need to cleanup later.===========
        self.log.info("Creating poke dict:")
        task_dict = {}
        TI = models.TaskInstance
        tis = session.query(TI)\
            .filter(TI.operator == self.sensor_operator) \
            .filter(or_(
            TI.state == State.SMART_RUNNING,
            TI.state == State.SMART_PENDING))\
            .all()

        for ti in tis:
            try:
                persist_info = yaml.full_load(ti.attr_dict)

                # if conn_id in self.poke_dict:
                #     self.poke_dict[conn_id][(schema, table, partition_name)] = \
                #         self.poke_dict[conn_id].get((schema, table, partition_name), "waiting")
                #
                #     self.task_dict[(conn_id, schema, table, partition_name)][-1]\
                #         .append((ti.dag_id, ti.task_id, ti.execution_date))
                # else:
                #     self.poke_dict[conn_id] = {(schema, table, partition_name): "waiting"}

                task_dict[(ti.dag_id, ti.task_id, ti.execution_date)] = (persist_info, ti.hashcode)

                # Change task instance state to mention this is picked up by a smart sensor
                if ti.state == State.SMART_PENDING:
                    ti.state = State.SMART_RUNNING
                    ti.start_date = timezone.utcnow()
                    session.commit()    # Need to avoid blocking DB for big query. Can Change to use chunk later
                    self.log.info("Set task {} to smart_running".format(ti.task_id))
            except Exception as e:
                self.log.info(e)

        self.log.info("Poke dict is: {}".format(str(task_dict)))
        return task_dict


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

    @provide_session
    def poke(self, session=None):

        poke_dict = self.init_poke_dict()
        poked = {}
        num_landed = 0

        self.log.info("Smart named hive partition sensor detect {} sensor tasks".format(len(poke_dict)))

        for (dag_id, task_id, execution_date) in poke_dict:
            key = (dag_id, task_id, execution_date)
            try:
                sensor_context = poke_dict[key][0]
                hashcode = poke_dict[key][1]

                metastore_conn_id = sensor_context.get('metastore_conn_id')
                hook = sensor_context.get('hook', None)
                partition_names = [
                    partition_name for partition_name in sensor_context['partition_names']
                    if not self.poke_partition(hook, partition_name, metastore_conn_id)
                ]

                if not partition_names:
                    num_landed += 1
                    self.set_state(dag_id, task_id, execution_date, State.SUCCESS)

                
            except Exception as e:
                self.log.error(e)
                key = (dag_id, task_id, execution_date)
                self.failed_dict[key] = self.failed_dict.get(key, 0) + 1
                if self.failed_dict[key] > max(3, sensor_context.get('retries')):
                    self.set_state(dag_id, task_id, execution_date, State.FAILED)
                    if sensor_context.get('soft_fail'):
                        # Handle soft fail
                        pass

        self.log.info("Number of landed is {}".format(num_landed))
        return len(poke_dict) == num_landed
