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

from builtins import range
from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.configuration import conf
from airflow.sensors.smart_named_hive_partition_sensor import SmartNamedHivePartitionSensor

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='smart_sensor_group',
    default_args=args,
    schedule_interval='0 0 * * *',
    # dagrun_timeout=timedelta(minutes=60),
)

if conf.has_option('core', 'use_smart_sensor') and conf.getboolean('core', 'use_smart_sensor'):
    num_smart_sensor_shard = conf.getint("smart_sensor", "shard_number")

for i in range(num_smart_sensor_shard):
    task = SmartNamedHivePartitionSensor(
        task_id='smart_named_hive_partition_sensor_' + str(i),
        dag=dag,
        retries=9999,
        retry_delay=timedelta(seconds=30),
        priority_weight=999,
    )

if __name__ == "__main__":
    dag.cli()
