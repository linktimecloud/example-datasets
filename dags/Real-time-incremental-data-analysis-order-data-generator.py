import random
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'kdp_demo_order_data_insert',
    description='Insert into orders by using random data',
    schedule_interval=timedelta(minutes=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['kdp-example'],
)

# MySQL connection info
mysql_host = 'kdp-data-mysql'
mysql_db = 'kdp_demo'
mysql_user = 'bdos_dba'
mysql_password = 'KdpDba!mysql123'
mysql_port = '3306'

cities = ["北京", "上海", "广州", "深圳", "成都", "杭州", "重庆", "武汉", "西安", "苏州", "天津", "南京", "郑州",
          "长沙", "东莞", "青岛", "宁波", "沈阳", "昆明", "合肥", "大连", "厦门", "哈尔滨", "福州", "济南", "温州",
          "佛山", "南昌", "长春", "贵阳", "南宁", "金华", "石家庄", "常州", "泉州", "南通", "太原", "徐州", "嘉兴",
          "乌鲁木齐", "惠州", "珠海", "扬州", "兰州", "烟台", "汕头", "潍坊", "保定", "海口"]
city = random.choice(cities)
consumer_id = random.randint(1, 100)
order_revenue = random.randint(1, 100)

# 插入数据的 BashOperator
insert_data_orders = BashOperator(
    task_id='insert_data_orders',
    bash_command=f'''
    mysql -h {mysql_host} -P {mysql_port} -u {mysql_user} -p{mysql_password} {mysql_db} -e "
    INSERT INTO orders(order_revenue,order_region,customer_id) VALUES({order_revenue},'{city}',{consumer_id});"
    ''',
    dag=dag,
)

insert_data_orders
