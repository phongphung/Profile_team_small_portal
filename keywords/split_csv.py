__author__ = 'sunary'


import pandas as pd
from utils import my_helper


class SplitCSV():

    def __init__(self):
        pass

    def split(self, file_source, path_dest, field_name, value_split):
        original_fields = my_helper.get_dataframe_columns(file_source)

        original_dataframe = pd.read_csv(file_source)
        generate_dataframe = [{} for _ in range(len(value_split))]

        for dataframe in generate_dataframe:
            for field in original_fields:
                dataframe[field] = []

        for i in range(len(original_dataframe[original_fields[0]])):
            for j in range(len(value_split)):
                if value_split[j] == original_dataframe[field_name][i]:
                    for k in range(len(original_fields)):
                        try:
                            generate_dataframe[j][original_fields[k]].append(my_helper.except_pandas_value(original_dataframe[original_fields[k]][i]))
                        except:
                            generate_dataframe[j][original_fields[k]].append(' ')


        generate_csv_name = [path_dest + value_split[i] + '.csv' for i in range(len(value_split))]
        for i in range(len(generate_csv_name)):
            original_dataframe = pd.DataFrame(data=my_helper.dataframe_same_length(generate_dataframe[i], original_fields), columns=original_fields)
            original_dataframe.to_csv(generate_csv_name[i], index= False)


if __name__ == '__main__':
    split = SplitCSV()
    split.split('/home/nhat/data/quality.csv', '/home/nhat/data/quality_new_', 'case', ['1', '2.1', '2.2.a', '2.2.b', '2.2.c', '2.3', '3.a', '3.b'])