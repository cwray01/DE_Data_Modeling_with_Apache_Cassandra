"""
This module provides Preparer classes to
prepare the raw files and make them ready
to be stored in the database.
"""
import os
import numpy as np
import pandas as pd
from utils import read_csv_file

def _types_to_native(values):
    """
    Converts numpy types to native types.
    """
    native_values = values.apply(
        lambda x: x.items() if isinstance(x, np.generic) else x, axis=1)
    native_values = native_values.where(native_values.notnull(), None)
    return native_values

# 类型转换
def _dataframe_to_dict(dataframe):
    """
    Converts pandas dataframe to dictionary.
    """
    return dataframe.to_dict("records")

# 定义Prepare类
class Preparer:
    """
    A generic preparer class.
    """

    # def prepare(self, values):
    #     """
    #     Applies table specific preparation.
    #     """
    #     raise NotImplementedError

    def format(self, values):
        """
        Applies any post formatting, if necessary.
        """
        pass

    def transform(self, values):
        """
        Prepares the file, converts to native data types and converts to
        dictionary.
        """
        # isinstance(values, type)判断value是否为type类型，返回boolean
        if isinstance(values, pd.Series):
            values = pd.DataFrame(values).T
            # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.T.html
            # 数据类型转换，将传入的values转为panda的DataFrame类型
        # 用的是自己实例的prepare方法
        transformed_values = self.prepare(values)
        transformed_values = _types_to_native(transformed_values)
        transformed_values = _dataframe_to_dict(transformed_values)
        self.format(transformed_values)
        # 用自己实例里的format方法，非必须
        return transformed_values

class PreparerQuery1(Preparer):
    """
    A preparer class for the query_1 table.
    """

    def prepare(self, values):
        #重写Preparer中的prepare方法
        #抽取的第一个query的列名
        # 获取存储在数据结构里的元素，用values属性获取所有元素
        # frame.values
        prepared_values = values[["artist", "song", "length", "sessionId",
                                  "itemInSession"]]

        # frame.columns
        #入库的列名，不涉及varchar转int
        # 重新指定每一列的名字
        # prepared_values.columns = ["artist_name", "song_title", "song_duration",
        #                            "session_id", "item_in_session"]
        return prepared_values

class PreparerQuery2(Preparer):
    def prepare(self, values):
        prepared_values = values[["userId", "sessionId", "artist",
                                  "song", "itemInSession", "firstName", "lastName"]]
        return prepared_values

    def format(self, values):
        for value in values:
            value["userId"] = int(value["userId"]) \
                if value["userId"] is not None else None

class PreparerQuery3(Preparer):

    def prepare(self, values):
        prepared_values = values[["song","firstName","lastName","userId"]]

        return prepared_values

    def format(self, values):
        for value in values:
            value["userId"] = int(value["userId"]) \
                if value["userId"] is not None else None



def main():
    preparer = PreparerQuery1()
    # print(os.getcwd())
    values = read_csv_file("../data/event_data_new.csv")
    print(values.to_string)
    isdf = isinstance(values, pd.DataFrame)
    # print(isdf)
    print("----------------------")

    pp_values = preparer.prepare(values)
    print(pp_values.to_string)

    print("----------------------*****")

    # print(values["artist"])
    print(preparer.prepare(values)["artist_name"])


if __name__ == "__main__":
    main()