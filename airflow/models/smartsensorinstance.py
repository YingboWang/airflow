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
from airflow.exceptions import (
    AirflowException, AirflowTaskTimeout, AirflowSkipException, AirflowRescheduleException, TaskNotFound
)
from airflow.utils.db import provide_session
from airflow.utils.state import State
from airflow.models.log import Log
from airflow.models.taskfail import TaskFail
from airflow.stats import Stats
from airflow import settings

import dill
import copy
import signal
import time
import os
import getpass

from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname

# from airflow.models.log import Log

class SmartSensorInstance(Base, LoggingMixin):
    """
    Used to save sensor task instances
    """

    __tablename__ = "smart_sensor_instance"

    id = Column(Integer, primary_key=True)
    smart_sensor_id = Column(String(250), nullable=False)
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
        self.task = task
        self.operator = operator or task.sensor_operator
        self.state = state
        self.queue = queue
        self.pool = pool
        self.priority_weight = 100  # temp setting large priority_weight
        self.hostname = ''
        self.unixname = getpass.getuser()
        self.executor_config = executor_config
        self.smart_sensor_id = task.task_id
        self.run_as_user = None

    @provide_session
    def _check_and_change_state_before_execution(
            self,
            verbose=True,
            mark_success=False,
            test_mode=False,
            job_id=None,
            pool=None,
            session=None):
        """
        Checks dependencies and then sets state to RUNNING if they are met. Returns
        True if and only if state is set to RUNNING, which implies that task should be
        executed, in preparation for _run_raw_task

        :param verbose: whether to turn on more verbose logging
        :type verbose: bool
        :param mark_success: Don't run the task, mark its state as success
        :type mark_success: bool
        :param test_mode: Doesn't record success or failure in the DB
        :type test_mode: bool
        :param pool: specifies the pool to use to run the task instance
        :type pool: str
        :return: whether the state was changed to running or not
        :rtype: bool
        """
        task = self.task
        self.pool = pool or task.pool
        self.test_mode = test_mode
        self.refresh_from_db(session=session, lock_for_update=True)
        self.job_id = job_id
        self.hostname = get_hostname()
        self.operator = task.sensor_operator

        # Another worker might have started running this task instance while
        # the current worker process was blocked on refresh_from_db
        if self.state == State.RUNNING:
            self.log.warning("Task Instance already running %s", self)
            session.commit()

            return False

        # if not test_mode:
        #     session.add(Log(State.RUNNING, self))
        self.state = State.RUNNING
        self.pid = os.getpid()
        self.end_date = None
        if not test_mode:
            session.merge(self)
        session.commit()

        # Closing all pooled connections to prevent
        # "max number of connections reached"
        settings.engine.dispose()
        if verbose:
            if mark_success:
                self.log.info("Marking success for smart sensor %s", self.task)
            else:
                self.log.info("Executing smart sensor %s", self.task)
        return True

    @provide_session
    def find(self, id, session=None):
        SI = SmartSensorInstance
        si = session.query(SI).filter(SI.id == self.id).one()

        return si

    @provide_session
    def _run_raw_task(
            self,
            mark_success=False,
            test_mode=False,
            job_id=None,
            pool=None,
            session=None):
        """
        Immediately runs the task (without checking or changing db state
        before execution) and then sets the appropriate final state after
        completion and runs any post-execute callbacks. Meant to be called
        only after another function changes the state to running.

        :param mark_success: Don't run the task, mark its state as success
        :type mark_success: bool
        :param test_mode: Doesn't record success or failure in the DB
        :type test_mode: bool
        :param pool: specifies the pool to use to run the task instance
        :type pool: str
        """
        task = self.task
        self.pool = pool or task.pool
        self.test_mode = test_mode
        self.refresh_from_db(session=session)
        self.job_id = job_id
        self.hostname = get_hostname()
        self.operator = task.sensor_operator

        context = {}
        actual_start_date = timezone.utcnow()
        try:
            if not mark_success:
                # context = self.get_template_context()

                task_copy = copy.copy(task)
                self.task = task_copy

                def signal_handler(signum, frame):
                    self.log.error("Received SIGTERM. Terminating subprocesses.")
                    task_copy.on_kill()
                    raise AirflowException("Task received SIGTERM signal")
                signal.signal(signal.SIGTERM, signal_handler)

                start_time = time.time()

                result = None
                if task_copy.execution_timeout:
                    try:
                        with timeout(int(
                                task_copy.execution_timeout.total_seconds())):
                            result = task_copy.execute(context=context)
                    except AirflowTaskTimeout:
                        task_copy.on_kill()
                        raise
                else:
                    result = task_copy.execute(context=context)

                end_time = time.time()
                duration = end_time - start_time
                # Stats.timing(
                #     'dag.{dag_id}.{task_id}.duration'.format(
                #         dag_id=task_copy.dag_id,
                #         task_id=task_copy.task_id),
                #     duration)
                #
                # Stats.incr('operator_successes_{}'.format(
                #     self.task.__class__.__name__), 1, 1)
                # Stats.incr('ti_successes')
            self.refresh_from_db(lock_for_update=True)
            self.state = State.SUCCESS
        except AirflowSkipException:
            self.refresh_from_db(lock_for_update=True)
            self.state = State.SKIPPED
        except AirflowException as e:
            self.refresh_from_db()
            # for case when task is marked as success/failed externally
            # current behavior doesn't hit the success callback
            if self.state in {State.SUCCESS, State.FAILED}:
                return
            else:
                self.handle_failure(e, test_mode, context)
                raise
        except (Exception, KeyboardInterrupt) as e:
            print(e)
            self.handle_failure(e, test_mode, context)
            raise

        # # Success callback
        # try:
        #     if task.on_success_callback:
        #         task.on_success_callback(context)
        # except Exception as e3:
        #     self.log.error("Failed when executing success callback")
        #     self.log.exception(e3)

        # Recording SUCCESS
        self.end_date = timezone.utcnow()
        self.set_duration()
        if not test_mode:
            # session.add(Log(self.state, self))
            session.merge(self)
        print("Commiting ssi with id: {}".format(self.id))
        session.commit()

    @provide_session
    def handle_failure(self, error, test_mode=False, context=None, session=None):
        self.log.exception(error)
        task = self.task
        self.end_date = timezone.utcnow()
        self.set_duration()
        Stats.incr('operator_failures_{}'.format(task.__class__.__name__), 1, 1)
        Stats.incr('ti_failures')

        '''===============================================
        handle_failure for smart_sensor: set to Failed state. no retry logic, no callback so far.
        can be adjust in next iteration.
        '''
        if not test_mode:
            session.add(Log(State.FAILED, self))

        # Log failure duration
        session.add(TaskFail(task, self.execution_date, self.start_date, self.end_date))

        self.state = State.FAILED

        if not test_mode:
            session.merge(self)

        session.commit()

    @provide_session
    def set_state(self, state, session=None):
        """
        Sensor record can be set as "waiting", "landed" and "invalid"
        :param state:
        :param session:
        :return:
        """
        SSI = SmartSensorInstance.refresh_from_db()
        self.state = state
        session.merge(self)
        session.commit()

    @staticmethod
    def generate_command(smart_sensor_id,
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
        cmd = ["airflow", "smart_sensor", str(operator_class), str(smart_sensor_id)]
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
        return cmd

    @provide_session
    def refresh_from_db(self, session=None, lock_for_update=False):
        """
        Refreshes the task instance from the database based on the primary key

        :param lock_for_update: if True, indicates that the database should
            lock the SmartSensorInstance (issuing a FOR UPDATE clause) until the
            session is committed.
        """
        SSI = SmartSensorInstance

        qry = session.query(SSI).filter(
            # SSI.id == self.id,
            SSI.smart_sensor_id == self.smart_sensor_id,
            )

        if lock_for_update:
            ssi = qry.with_for_update().first()
        else:
            ssi = qry.first()
        if ssi:
            # self.task = ssi
            self.id = ssi.id
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

    # def init_run_context(self, raw=False):
    #     """
    #     Sets the log context.
    #     """
    #     self.raw = raw
    #     self._set_context(self)

    def set_duration(self):
        if self.end_date and self.start_date:
            self.duration = (self.end_date - self.start_date).total_seconds()
        else:
            self.duration = None

    def command_as_list(
            self,
            mark_success=False,
            ignore_all_deps=False,
            ignore_task_deps=False,
            ignore_depends_on_past=False,
            ignore_ti_state=False,
            local=False,
            pickle_id=None,
            raw=False,
            job_id=None,
            pool=None,
            cfg_path=None):
        """
        Returns a command that can be executed anywhere where airflow is
        installed. This command is part of the message sent to executors by
        the orchestrator.
        """

        cmd = SmartSensorInstance.generate_command(
            self.smart_sensor_id,

            mark_success=mark_success,
            ignore_all_deps=ignore_all_deps,
            ignore_task_deps=ignore_task_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_ti_state=ignore_ti_state,
            local=local,
            pickle_id=pickle_id,
            raw=raw,
            job_id=job_id,
            pool=pool,
            operator_class = self.operator,
            cfg_path=cfg_path)

        print(cmd)
        return cmd
