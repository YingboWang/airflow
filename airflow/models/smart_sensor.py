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

from sqlalchemy import Column, Integer, String, Text, Index

from airflow.models.base import Base, ID_LEN
from airflow.utils import timezone
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.db import provide_session
from airflow.utils.state import State
from airflow import settings

# from airflow.models.log import Log

class SensorInstance(Base):
    """
    Used to save sensor task instances
    """

    __tablename__ = "sensor_item"

    id = Column(Integer, primary_key=True)
    item_hash = Column(String(2000))    # Used for item deduplicate.
    dttm = Column(UtcDateTime)
    operator = Column(String(1000))
    conn_id = Column(String(ID_LEN))
    # =========== May use a dict for sensor arg dictionary or split into each partitions/location
    schema = Column(String(1000))
    table = Column(String(1000))
    partition = Column(String(1000))
    context = Column(String(2000))
    state = Column(String(20))

    __table_args__ = (
        Index('idx_id', id),
        Index('idx_operator'),
        Index('idx_conn_id', conn_id),
    )

    def __init__(self,
            item_hash=None,
            operator=None,
            conn_id=None,
            schema='default',
            table = None,
            partition = None,
            *args,
            **kwargs):

        self.dttm = timezone.utcnow()
        self.item_hash = item_hash
        self.operator = operator
        self.conn_id = conn_id
        self.schema = schema
        self.table = table
        self.partition = partition

        # if not isinstance(sensor_instance, BaseSensorOperator):
        #     # Can not init sensor record with task_instance other than sensor
        #     return
        # if sensor_instance:
        #     self.dag_id = sensor_instance.dag_id
        #     self.task_id = sensor_instance.task_id
        #     self.execution_date = sensor_instance.execution_date
        #     self.operator = sensor_instance.operator
        #
        #     task_owner = sensor_instance.task.owner


        if 'task_id' in kwargs:
            self.task_id = kwargs['task_id']
        if 'dag_id' in kwargs:
            self.dag_id = kwargs['dag_id']
        if 'execution_date' in kwargs:
            if kwargs['execution_date']:
                self.execution_date = kwargs['execution_date']

        self.conn_id = kwargs['conn_id']

    @provide_session
    def find(self, id, session=None):
        SI = SensorInstance
        si = session.query(SI).filter(SI.id == self.id).one()

        return si

    @provide_session
    def set_state(self, state, session=None):
        """
        Sensor record can be set as "waiting", "landed" and "invalid"
        :param state:
        :param session:
        :return:
        """
        SI = SensorInstance
        self.state = state
        session.merge(self)
        session.commit()


class SmartSensorWrap(Base):
    """
    Used to save sensor task instances and sensor item relation
    """

    __tablename__ = "smart_sensor_dependency"

    id = Column(Integer, primary_key=True)
    ti_id = Column(String(1000))    # hashcode <dag_id, task_id, execution_date> for task_instance
    si_id = Column(String(1000))    # hashcode for sensor item.
    state = Column(String(20))

    __table_args__ = (
        Index('idx_id', id),
        Index('idx_ti', ti_id),
        Index('idx_si', si_id),
    )

    def __init__(self, id=None, ti_id=None, si_id=None, state=None):
        self.id = id
        self.ti_id = ti_id
        self.si_id = si_id
        self.state = state

    @provide_session
    def set_sensor_item(self, si_id, state, session=None):
        """
        Set sensor item state in smart sensor dependency table
        :param si_id:
        :param state:
        :param session:
        :return:
        """
        rows = session.query(SmartSensorWrap).filter(SmartSensorWrap.si_id == si_id)
        for row in rows:
            row.state = state
            session.merge(row)

        session.commit()

    @provide_session
    def set_si_success_renew_success_ti(self, si_id, session=None):
        """
        Set the specified sensor item landed and return succeed tis list
        :param si_id:
        :param session:
        :return:
        """
        ti_ids = []
        rows = session.query(SmartSensorWrap).filter(SmartSensorWrap.si_id == si_id)
        for row in rows:
            row.state = State.SUCCESS
            sensor_waiting_item = session.query(SmartSensorWrap) \
                .filter(SmartSensorWrap.ti_id == row.ti_id,
                        SmartSensorWrap.state != State.SUCCESS,
                        SmartSensorWrap.si_id != si_id) \
                .first()
            if sensor_waiting_item is None:
                ti_ids.append(row.ti_id)
            session.merge(row)

        session.commit()

        for ti_id in ti_ids:
            # Set task instance state.
            pass
        return ti_ids


    @provide_session
    def is_sensor_task_succeed(self, dag_id, task_id, execution_date, session=None):
        """
        Check if the sensor task dependencies (with sensor items) are fulfilled.
        :param dag_id:
        :param task_id:
        :param execution_date:
        :param session:
        :return:
        """

        #===========Need to overwrite the hashcode functoin to be independent with builtin=============
        ti_id = hash(dag_id, task_id, execution_date)
        sensor_waiting_item = session.query(SmartSensorWrap) \
            .filter(SmartSensorWrap.ti_id == ti_id, SmartSensorWrap.state != State.SUCCESS).first()

        return False if sensor_waiting_item else True

    @provide_session
    def get_all_succeed_tasks(self):

        pass
