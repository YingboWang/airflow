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


from time import sleep
from datetime import timedelta

from airflow.exceptions import AirflowException, AirflowSensorTimeout, \
    AirflowSkipException, AirflowRescheduleException
from airflow.models import BaseOperator, TaskInstance
from airflow.models.skipmixin import SkipMixin
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults
from airflow.utils.db import provide_session
from sqlalchemy import (Column, Index, Integer, String, and_, func, not_, or_)
from airflow.utils.state import State
import yaml


class BaseSmartOperator(BaseOperator, SkipMixin):
    """
    Sensor operators are derived from this class and inherit these attributes.

    Sensor operators keep executing at a time interval and succeed when
    a criteria is met and fail if and when they time out.

    :param soft_fail: Set to true to mark the task as SKIPPED on failure
    :type soft_fail: bool
    :param poke_interval: Time in seconds that the job should wait in
        between each tries
    :type poke_interval: int
    :param timeout: Time, in seconds before the task times out and fails.
    :type timeout: int
    :type mode: str
    """
    ui_color = '#e6f1f2'

    @apply_defaults
    def __init__(self,
                 poke_interval=60,
                 timeout=60 * 60 * 24 * 7,
                 soft_fail=False,
                 *args,
                 **kwargs):
        super(BaseSmartOperator, self).__init__(*args, **kwargs)
        self.poke_interval = poke_interval
        self.soft_fail = soft_fail
        self.timeout = timeout
        self._validate_input_values()
        self.task_dict = {}
        self.poked_dict = {}
        self.failed_hash_dict = {}
        self.clear_set = set()
        self.max_tis_per_query = 50

    def _validate_input_values(self):
        if not isinstance(self.poke_interval, (int, float)) or self.poke_interval < 0:
            raise AirflowException(
                "The poke_interval must be a non-negative number")
        if not isinstance(self.timeout, (int, float)) or self.timeout < 0:
            raise AirflowException(
                "The timeout must be a non-negative number")

    @provide_session
    def refresh_task_dict(self, session=None):
        """
        Init poke dictionary. Function that the sensors defined while deriving this class should
        override.
        """
        self.log.info("Creating poke dict:")
        task_dict = {}
        TI = TaskInstance
        tis = session.query(TI) \
            .filter(TI.operator == self.sensor_operator) \
            .filter(or_(
            TI.state == State.SMART_RUNNING,
            TI.state == State.SMART_PENDING)) \
            .all()

        for ti in tis:
            try:
                task_dict[(ti.dag_id, ti.task_id, ti.execution_date)] = \
                    (yaml.full_load(ti.attr_dict), ti.hashcode)

                # Change task instance state to mention this is picked up by a smart sensor
                if ti.state == State.SMART_PENDING:
                    ti.state = State.SMART_RUNNING
                    ti.start_date = timezone.utcnow()
                    session.commit()  # Need to avoid blocking DB for big query. Can Change to use chunk later
                    self.log.info("Set task {} to smart_running".format(ti.task_id))
            except Exception as e:
                self.log.info(e)

        self.log.info("Poke dict is: {}".format(str(task_dict)))
        return task_dict

    def refresh_all_dict(self):
        self.task_dict = self.refresh_task_dict()
        self.poked_dict = {}
        for item in self.clear_set:
            try:
                del self.failedd_hash_dict[item]
            except KeyError:
                pass
        self.clear_set = set()

    def poke(self, poke_context):
        """
        Function that the sensors defined while deriving this class should
        override.
        """
        raise AirflowException('Override me.')

    @provide_session
    def mark_state(self, poke_hash, state, session=None):
        TI = TaskInstance
        tis = session.query(TI)\
            .filter(TI.hashcode == poke_hash,
                    TI.operator == self.sensor_operator)\
            .filter(or_(TI.state == State.SMART_RUNNING,
                        TI.state == State.SMART_PENDING))\
            .all()
        self.log.info("Found {} tasks for hashcode {}".format(len(tis), poke_hash))
        chunks = [tis[i: i + self.max_tis_per_query] for i in range(0, len(tis), self.max_tis_per_query)]

        for chunk in chunks:
            try:
                end_date = timezone.utcnow()
                for ti in chunk:
                    ti.state = state
                    ti.end_date = end_date
                    session.merge(ti)
                session.commit()
                self.log.info("Mark state for {}, {}, {} to {}".format(ti.dag_id, ti.task_id,ti.execution_date, state))
            except Exception as e:
                self.logger.exception("Exception mark_state in smart sensor: {}, hashcode: {}" \
                                      .format(str(e), poke_hash))
        self.log.info("Mark {} task to state {}".format(len(tis), state))

    @provide_session
    def set_state(self, dag_id, task_id, execution_date, state, session=None):
        TI = TaskInstance
        ti = session.query(TI).filter(
            TI.dag_id == dag_id,
            TI.task_id == task_id,
            TI.execution_date == execution_date
        ).one()
        if ti and ti.state != state:
            ti.state = state
            ti.end_date = timezone.utcnow()
            session.merge(ti)
            session.commit()
        else:
            self.log.warning("The task instance need to be set can not be found")

    def execute(self, context):
        started_at = timezone.utcnow()
        # while (timezone.utcnow() - started_at).total_seconds() > self.timeout:
        while True:
            if (timezone.utcnow() - started_at).total_seconds() > self.timeout:
                # If sensor is in soft fail mode but will be retried then
                # give it a chance and fail with timeout.
                # This gives the ability to set up non-blocking AND soft-fail sensors.
                if self.soft_fail and not context['ti'].is_eligible_to_retry():
                    self._do_skip_downstream_tasks(context)
                    raise AirflowSkipException('Snap. Time is OUT.')
                else:
                    raise AirflowSensorTimeout('Snap. Time is OUT.')

            self.log.info("Refresh all dict for smart sensor")
            self.refresh_all_dict()

            for key in self.task_dict:
                poke_context, poke_hash = self.task_dict[key]
                if poke_hash in self.poked_dict:
                    continue

                try:
                    if self.poke(poke_context):

                        self.poked_dict[poke_hash] = 0
                        self.mark_state(poke_hash, State.SUCCESS)
                    else:
                        self.poked_dict[poke_hash] = 1
                except Exception as e:
                    self.log.info(e)
                    self.poked_dict[poke_hash] = 2
                    self.failed_hash_dict[poke_hash] = self.failed_hash_dict.get(poke_hash, 0) + 1
                    if self.failed_hash_dict[poke_hash] > max(3, poke_context.get('retries', 0)):
                        self.mark_state(poke_hash, State.FAILED)
                        self.clear_set.add(poke_hash)
                        self.log.info("Add {} to clear_set".format(poke_hash))

            sleep(self.poke_interval)

        raise AirflowSkipException('Snap. Time is OUT.')

    def _do_skip_downstream_tasks(self, context):
        # This function is not called in current smart sensor but related to soft_fail handle.
        # todo: refactor to handle soft_fail in smart sensor. Keep it for now as reminder
        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
        self.log.debug("Downstream task_ids %s", downstream_tasks)
        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)
