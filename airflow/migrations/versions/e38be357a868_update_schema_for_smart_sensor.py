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

"""update schema for smart sensor

Revision ID: e38be357a868
Revises: 939bb1e647c8
Create Date: 2019-06-07 04:03:17.003939

"""
from alembic import op
import sqlalchemy as sa
import dill


# revision identifiers, used by Alembic.
revision = 'e38be357a868'
down_revision = '939bb1e647c8'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('task_instance', sa.Column('attr_dict', sa.Text(), nullable=True))
    op.create_table('smart_sensor_instance',
                    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
                    sa.Column('item_hash', sa.String(length=2000), nullable=True),
                    sa.Column('smart_sensor_id', sa.String(length=250), nullable=False),
                        sa.Column('start_date', sa.DATETIME(timezone=True)),
                        sa.Column('end_date', sa.DATETIME(timezone=True)),
                        sa.Column('duration', sa.Float),
                        sa.Column('state', sa.String(length=20)),
                        sa.Column('hostname', sa.String(length=1000)),
                        sa.Column('unixname', sa.String(length=1000)),
                        sa.Column('job_id', sa.Integer),
                        sa.Column('pool', sa.String(length=50)),
                        sa.Column('queue', sa.String(length=50)),
                        sa.Column('priority_weight', sa.Integer),
                        sa.Column('operator', sa.String(length=1000)),
                        sa.Column('queued_dttm', sa.DATETIME(timezone=True)),
                    sa.Column('pid', sa.Integer),
                    sa.Column('executor_config', sa.PickleType(pickler=dill)),
                    sa.PrimaryKeyConstraint('id'))


def downgrade():
    op.drop_column('task_instance', 'context')
    op.drop_table('smart_sensor_instance')
