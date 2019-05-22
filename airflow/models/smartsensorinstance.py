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

from sqlalchemy import Column, Integer, String, Text, Index, Float, PickleType

from airflow.models.base import Base, ID_LEN
from airflow.utils import timezone
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.db import provide_session
from airflow.utils.state import State
from airflow import settings
import dill

# from airflow.models.log import Log

class SmartSensorInstance(Base):
    """
    Used to save sensor task instances
    """

    __tablename__ = "smart_sensor_instance"

    id = Column(Integer, primary_key=True)
    start_date = Column(UtcDateTime)
    end_date = Column(UtcDateTime)
    duration = Column(Float)
    state = Column(String(20))
    hostname = Column(String(1000))
    unixname = Column(String(1000))
    job_id = Column(Integer)
    pool = Column(String(50))
    queue = Column(String(50))
    priority_weight = Column(Integer)
    operator = Column(String(1000))
    queued_dttm = Column(UtcDateTime)
    pid = Column(Integer)
    executor_config = Column(PickleType(pickler=dill))

    __table_args__ = (
        Index('idx_id', id),
        Index('idx_operator'),
        Index('idx_job_id', job_id),
    )

    def __init__(self,
            operator,
            task,
            state=None,
            queue = "default",
            pool = None,
            executor_config = None,
            *args,
            **kwargs):

        # Be careful about the id, may use transient.
        self.start_date = timezone.utcnow()
        self.operator = operator or task.sensor_operator
        self.state = state
        self.queue = queue
        self.pool = pool
        self.priority_weight = 100  # temp setting large priority_weight
        self.hostname = ''
        self.executor_config = executor_config
        self.id = task.task_id


    @provide_session
    def find(self, id, session=None):
        SI = SmartSensorInstance
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
        SSI = SmartSensorInstance
        self.state = state
        session.merge(self)
        session.commit()

    @staticmethod
    def generate_command(task_id,
                         operator_class,
                         mark_success=False,
                         ignore_all_deps=False,
                         ignore_depends_on_past=False,
                         ignore_task_deps=False,
                         ignore_ti_state=False,
                         local=False,
                         pickle_id=None,
                         file_path=None,
                         raw=False,
                         job_id=None,
                         pool=None,
                         cfg_path=None
                         ):
        """
        Generates the shell command required to execute this task instance.

        :param dag_id: DAG ID
        :type dag_id: unicode
        :param task_id: Task ID
        :type task_id: unicode
        :param execution_date: Execution date for the task
        :type execution_date: datetime
        :param mark_success: Whether to mark the task as successful
        :type mark_success: bool
        :param ignore_all_deps: Ignore all ignorable dependencies.
            Overrides the other ignore_* parameters.
        :type ignore_all_deps: bool
        :param ignore_depends_on_past: Ignore depends_on_past parameter of DAGs
            (e.g. for Backfills)
        :type ignore_depends_on_past: bool
        :param ignore_task_deps: Ignore task-specific dependencies such as depends_on_past
            and trigger rule
        :type ignore_task_deps: bool
        :param ignore_ti_state: Ignore the task instance's previous failure/success
        :type ignore_ti_state: bool
        :param local: Whether to run the task locally
        :type local: bool
        :param pickle_id: If the DAG was serialized to the DB, the ID
            associated with the pickled DAG
        :type pickle_id: unicode
        :param file_path: path to the file containing the DAG definition
        :param raw: raw mode (needs more details)
        :param job_id: job ID (needs more details)
        :param pool: the Airflow pool that the task should run in
        :type pool: unicode
        :param cfg_path: the Path to the configuration file
        :type cfg_path: basestring
        :return: shell command that can be used to run the task instance
        """
        cmd = ["airflow", "smart_sensor", str(operator_class), str(task_id)]
        cmd.extend(["--mark_success"]) if mark_success else None
        cmd.extend(["--pickle", str(pickle_id)]) if pickle_id else None
        cmd.extend(["--job_id", str(job_id)]) if job_id else None
        cmd.extend(["-A"]) if ignore_all_deps else None
        cmd.extend(["-i"]) if ignore_task_deps else None
        cmd.extend(["-I"]) if ignore_depends_on_past else None
        cmd.extend(["--force"]) if ignore_ti_state else None
        cmd.extend(["--local"]) if local else None
        cmd.extend(["--pool", pool]) if pool else None
        cmd.extend(["--raw"]) if raw else None
        cmd.extend(["-sd", file_path]) if file_path else None
        cmd.extend(["--cfg_path", cfg_path]) if cfg_path else None
        return cmd

    @provide_session
    def refresh_from_db(self, session=None, lock_for_update=False):
        """
        Refreshes the task instance from the database based on the primary key

        :param lock_for_update: if True, indicates that the database should
            lock the TaskInstance (issuing a FOR UPDATE clause) until the
            session is committed.
        """
        SSI = SmartSensorInstance

        qry = session.query(SSI).filter(
            SSI.id == self.id,
            )

        if lock_for_update:
            ssi = qry.with_for_update().first()
        else:
            ssi = qry.first()
        if ssi:
            self.state = ssi.state
            self.start_date = ssi.start_date
            self.end_date = ssi.end_date
            # Get the raw value of try_number column, don't read through the
            # accessor here otherwise it will be incremeneted by one already.
            self.hostname = ssi.hostname
            self.pid = ssi.pid
            self.executor_config = ssi.executor_config
        else:
            self.state = None

