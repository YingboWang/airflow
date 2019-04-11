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
from airflow.sensors.metastore_partition_sensor import MetastorePartitionSensor
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from airflow.exceptions import AirflowException, AirflowSensorException
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import (Column, Index, Integer, String, and_, func, not_, or_)
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm.session import make_transient

class SmartMetastorePartitionSensor(BaseSmartOperator, MetastorePartitionSensor):
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
        self.sensor_operator = "MetastorePartitionSensor"
        self.poke_dict = {}
        self.task_dict = {}

    @provide_session
    def init_poke_dict(self, session=None):
        """
        Initiate poke dict based on operators. Need to improve for sharding big amount.
        :param operators:
        :return:
        """
        result = {}
        TI = models.TaskInstance
        tis = session.query(TI).filter(TI.operator == self.sensor_operator,
                                       TI.state in (State.SMART_PENDING, State.SMART_RUNNING)).all()

        for ti in tis:
            conn_id = ti.attr_dict["conn_id"]
            partition_name = ti.attr_dict["partition_name"]
            table = ti.attr_dict["table"]
            schema = ti.attr_dict["schema"]

            # if conn_id in self.poke_dict:
            #     self.poke_dict[conn_id][(schema, table, partition_name)] = \
            #         self.poke_dict[conn_id].get((schema, table, partition_name), "waiting")
            #
            #     self.task_dict[(conn_id, schema, table, partition_name)][-1]\
            #         .append((ti.dag_id, ti.task_id, ti.execution_date))
            # else:
            #     self.poke_dict[conn_id] = {(schema, table, partition_name): "waiting"}

            result[(ti.dag_id, ti.task_id, ti.execution_date)] = \
                (conn_id, schema, table, partition_name, ti.attr_dict)

            # Change task instance state to mention this is picked up by a smart sensor
            if ti.state == State.SMART_PENDING:
                ti.state = State.SMART_RUNNING
                session.commit()    # Need to avoid blocking DB for big query. Can Change to use chunk later
        return result

    @provide_session
    def poke(self, context, session=None):

        TI = models.TaskInstance
        self.poke_dict = self.init_poke_dict()
        # Can either use MetastorePartitionSensor or directly poke from ti.attr_dict
        num_landed = 0
        for (dag_id, task_id, execution_date) in self.poke_dict:
            conn_id, schema, table, partition_name, context = self.poke_dict[(dag_id, task_id, execution_date)]
            single_sensor = MetastorePartitionSensor(table, partition_name, schema, conn_id)
            try:
                landed = single_sensor.poke(context)
                if landed:
                    ti = session.query(TI).filter_by(
                        TI.dag_id == dag_id,
                        TI.task_id == task_id,
                        TI.execution_date == execution_date
                    ).first()
                    ti.state = State.SUCCESS
                    session.merge(ti)
                    session.commit()
                    num_landed += 1

            except AirflowException:
                self.log.error("SmartSensor poke exception on {} dag_id = {}, task_id = {}, execution_date = {}".
                               format(self.sensor_operator, dag_id, task_id, execution_date))
        return len(self.poke_dict) == num_landed


    @provide_session
    def poke(self, session=None):

        poke_dict = self.init_poke_dict()
        self.log.info("Smart metastore partition sensor detect {} sensor tasks".format(len(poke_dict)))
        TI = models.TaskInstance
        num_landed = 0

        for (dag_id, task_id, execution_date) in poke_dict:
            conn_id, schema, table, partition_name, context = poke_dict[(dag_id, task_id, execution_date)]
            if '.' in table:
                schema, table = table.split('.')
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

            if records and str(records[0][0]) not in ('0', ''):
                num_landed += 1
                ti = session.query(TI).filter_by(
                    TI.dag_id == dag_id,
                    TI.task_id == task_id,
                    TI.execution_date == execution_date
                ).first()
                ti.state = State.SUCCESS
                session.merge(ti)
                session.commit()
            return len(poke_dict) == num_landed


