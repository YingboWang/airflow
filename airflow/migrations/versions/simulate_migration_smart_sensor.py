from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'f23433877c25'
down_revision = '64de9cddf6c9'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('task_instance', sa.Column('context', sa.Text(), nullable=True))


def downgrade():
    op.drop_column('task_instance', 'context')


# def upgrade():
#     op.create_table('sensor_item',
#                     sa.Column('id', sa.Integer(), autoincrement=True),
#                     sa.Column('item_hash', sa.String(length=2000), nullable=False),
#                     sa.Column('dttm', sa.DATETIME(timezone=True)),
#                     sa.Column('operator', sa.String(length=1000)),
#                     sa.Column('conn_id', sa.String(length=1000)),
#                     sa.Column('schema', sa.String(length=1000)),
#                     sa.Column('table', sa.String(length=1000)),
#                     sa.Column('partition', sa.String(length=1000)),
#                     sa.Column('latest_refresh', sa.DATETIME(timezone=True)),
#                     sa.PrimaryKeyConstraint('id'))
#     op.create_table('smart_sensor_dependency',
#                     sa.Column('id', sa.Integer(), autoincrement=True),
#                     sa.Column('ti_id', sa.String(length=1000), nullable=False),
#                     sa.Column('dag_id', sa.String(length=1000)),
#                     sa.Column('task_id', sa.String(length=1000)),
#                     sa.Column('execution_date', sa.String(length=1000)),
#                     sa.Column('si_id', sa.String(length=1000)),
#                     sa.Column('state', sa.String(length=20)),
#                     sa.PrimaryKeyConstraint('id'))
#
#
# def downgrade():
#     op.drop_table('sensor_item')
#     op.drop_table('smart_sensor_dependency')
