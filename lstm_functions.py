import numpy as np
import pandas as pd

def create_sequences(data, normalize_col, x_features, y_feature, sequence_length, test_length, groupby_column=None):
    ''' data: original dataframe
        x_features: list of column names to create x_sequences from
        y_feature: y_feature as target of the sequnce
        sequence_length: length of the x_features data
        test_length: number of datapoints to be considered as test data
        groupby_column: column to be grouped by in case of separete units in the data'''
    # create the input data for each symbol seperately
    X_train, y_train, X_test, y_test = [], [], [], []
    if groupby_column is not None:
        # check if grouped feature is in index
        for i, group in enumerate(data[groupby_column].unique()):
            if i % 100 == 0:
                print(i, group)
            # create a dataframe for the symbol
            df_symbol = data.loc[data[groupby_column] == group].copy()
            df_symbol.sort_values('date', inplace=True)
            df_symbol.reset_index(drop=True, inplace=True)
            X_train_group, y_train_group, X_test_group, y_test_group = [], [], [], []
            # loop over all sequences of the symbol
            for start in range(df_symbol.shape[0] - sequence_length + 1):
                # create a dataframe for the sequence
                df_data = df_symbol.iloc[start : start + sequence_length].copy()
                # normalize data by current price
                norm_val = df_data.iloc[-1][normalize_col]
                df_temp = df_data[x_features].div(norm_val)
                df_data = pd.concat([df_data.drop(x_features, axis=1), df_temp], axis=1)
                # create test data for the last TEST_LEN sequences
                if start >= df_symbol.shape[0] - sequence_length + 1 - test_length:
                    X_test_group.append(df_data[x_features])
                    y_test_group.extend(df_data[y_feature].iloc[-1])
                # create train data for the rest sequences
                else:
                    X_train_group.append(df_data[x_features])
                    y_train_group.extend(df_data[y_feature].iloc[-1])
            # # add the symbol sequences to the total sequence
            X_train.extend(np.array(X_train_group))
            y_train.extend(np.array(y_train_group))
            X_test.extend(np.array(X_test_group))
            y_test.extend(np.array(y_test_group))
            print(np.array(X_train).shape, np.array(y_train).shape, np.array(X_test).shape, np.array(y_test).shape)
    # return np arrays instead of list of np.arrays
    return np.array(X_train), np.array(y_train), np.array(X_test), np.array(y_test)



def get_input_arrays(indices_dict, np_all, win_len, test_size, norm_col):
    '''iterate over grouped indices. last column must be y column
    creates x and y arrays. normalizes x array by last value of norm-col'''
    X_train, y_train, X_test, y_test = [], [], [], []
    for i, entry in enumerate(indices_dict.items()):
        test_start = entry[1][-1] - win_len - test_size + 2 # works
        X_train_isin, y_train_isin, X_test_isin, y_test_isin = [], [], [], []
        for start in entry[1][:-win_len + 1]:
            last = start + win_len - 1 # correct window
            x = np_all[start:last + 1, :-1] / np_all[last, norm_col]
            y = np_all[last, -1]
            if start >= test_start: 
                X_test_isin.append(x)        
                y_test_isin.append(y)
            else:
                X_train_isin.append(x)        
                y_train_isin.append(y)
        X_train.extend(np.array(X_train_isin))
        y_train.extend(np.array(y_train_isin))
        X_test.extend(np.array(X_test_isin))
        y_test.extend(np.array(y_test_isin))
        if i % 500 == 0:
            print(np.array(X_train).shape, np.array(y_train).shape, np.array(X_test).shape, np.array(y_test).shape)
    return np.array(X_train), np.array(y_train), np.array(X_test), np.array(y_test)
