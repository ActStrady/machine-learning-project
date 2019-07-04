import pymysql
import pandas as pd
from sklearn.linear_model import LinearRegression

import sys
import numpy

numpy.set_printoptions(threshold=sys.maxsize)


# 获取mysql连接
def get_connection():
    connection = pymysql.connect(
        user='root',
        password='root'
    )

    return connection


sql = 'select * from db_ml.train_data'

# 获取训练集
train = pd.read_sql(sql=sql, con=get_connection())

# 训练集特征
x = train.drop(labels='order_count', axis=1)
# 训练集标签
y = train[['order_count']]

# 训练
lr = LinearRegression()
lr.fit(x, y)

sql = 'select * from db_ml.test_data'

# 验证集
test = pd.read_sql(sql=sql, con=get_connection())
# 验证集特征
x_test = test[
    [
        "s_petrol", "s_market", "s_uptown", "s_metro", "s_bus",
        "s_cafe", "s_restaurant", "s_atm", "s_office", "s_hotel",
        "e_petrol", "e_market", "e_uptown", "e_metro", "e_bus",
        "e_cafe", "e_restaurant", "e_atm", "e_office", "e_hotel",
        "w_temperature", "w_feels_like", "w_pressure", "w_humidity",
        "w_visibility", "w_direction_degree", "w_wind_speed"
    ]
]

# 预测的订单数量
y_predict = lr.predict(x_test)

# 保存到csv里
df = pd.DataFrame(y_predict.reshape(-1, 1))
df.to_csv("data/result.csv")

# 得分 1.0为最好
print(lr.score(x_test, y_predict))
