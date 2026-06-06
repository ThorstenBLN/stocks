import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns
import numpy as np
import datetime
import os
import pickle

from sklearn.preprocessing import MinMaxScaler, PolynomialFeatures
from sklearn.metrics import confusion_matrix, accuracy_score

import tensorflow as tf
#f rom tensorflow import keras, initializers, device
# from tensorflow.keras.utils import to_categorical
from keras.models import Sequential
# from keras.layers import Dense, Dropout, LSTM, Activation, Bidirectional
from keras.callbacks import EarlyStopping
from keras import backend as K
from keras.metrics import Precision, Recall
# import io

SEQUENCE_LENGTH = 40 # optimized [40, 45]

N_EPOCHS = 6 # optimized [16, 18, 20]
N_BATCH_SIZE = 48 # optimized 48
CLASS_WEIGHTS = {0:3, 1:1, 2:1, 3:1} # oprtimized [{0: 1, 1: 2, 2: 2}, {0: 1, 1: 2, 2: 3}, {0: 1, 1: 3, 2: 4}, {0: 1, 1: 4, 2: 4}]

LSTM_1_UNITS = 64 # [48, 64, 80] # optimum: (DATASET 1: 32) (DATASET 2: 64)
LSTM_2_UNITS = 48 # [40, 48, 56] # optimum: (DATASET 1: 24) (DATASET 2: 48)
LSTM_3_UNITS = 32 # [24, 32, 40] # optimum: (DATASET 1: 24) (DATASET 2: 32)
PATH = './results/'

FILE_DATA = 'data_lstm_ind_4_classes.csv'
PATH_DATA = "./data_lstm/"

def get_input_arrays(indices_dict, np_all, win_len, test_size, val_size):
    '''iterate over grouped indices. last column must be y column
    creates x and y arrays. normalizes x array by last value of norm-col'''
    train_indices, val_indices, test_indices = {}, {}, {}
    X_train, y_train, X_val, y_val, X_test, y_test = [], [], [], [], [], []
    count = 0
    for i, entry in enumerate(indices_dict.items()):
        test_start = entry[1][-1] - win_len - test_size + 2 # works
        val_start = test_start - val_size
        X_train_isin, y_train_isin, X_val_isin, y_val_isin, X_test_isin, y_test_isin = [], [], [], [], [], []
        train_ind, val_ind, test_ind = [], [], []
        for start in entry[1][:-win_len + 1]:
            last = start + win_len - 1 # correct window
            x = np_all[start:last + 1, :-1].copy()
            y = np_all[last, -1]
            if start >= test_start:
                X_test_isin.append(x)
                y_test_isin.append(y)
                test_ind.append((count, start.item(), last.item()))
            elif start >= val_start:
                X_val_isin.append(x)
                y_val_isin.append(y)
                val_ind.append((count, start.item(), last.item()))
            else:
                X_train_isin.append(x)
                y_train_isin.append(y)
                train_ind.append((count, start.item(), last.item()))
            count += 1
        X_train.extend(np.array(X_train_isin))
        y_train.extend(np.array(y_train_isin))
        X_val.extend(np.array(X_val_isin))
        y_val.extend(np.array(y_val_isin))
        X_test.extend(np.array(X_test_isin))
        y_test.extend(np.array(y_test_isin))
        train_indices[entry[0]] = train_ind
        val_indices[entry[0]] = val_ind
        test_indices[entry[0]] = test_ind
        if i % 500 == 0:
            print(np.array(X_train).shape, np.array(y_train).shape, np.array(X_val).shape, 
                  np.array(y_val).shape, np.array(X_test).shape, np.array(y_test).shape)
    return np.array(X_train), np.array(y_train), np.array(X_val), np.array(y_val), np.array(X_test), np.array(y_test), train_indices, val_indices, test_indices

X_FEATURES = ['ret_1', 'ret_5', 'ret_20', 'RSI', 'ma_ratio_5', 'ma_ratio_10', 'ma_ratio_20', 
              'z_score_ret_1', 'z_score_ret_5', 'z_vol', 'z-volatility', 'div_norm',
              'dax_ret_1', 'dax_ret_5', 'dax_ma_ratio_20', 'dax_z-volatility', 
                      'msci_ret_1', 'msci_ret_5', 'msci_ma_ratio_20', 'msci_z-volatility',
                'dax_ret_1_cross', 'dax_ret_5_cross', 'msci_ret_1_cross', 'msci_ret_5_cross']
Y_FEATURES = ['class']

print(tf.config.list_physical_devices("GPU"))

 # load data
df_all = pd.read_csv(PATH_DATA + FILE_DATA)
df_all['date'] = pd.to_datetime(df_all['date'])
# cast dtypes to save RAM
df_all = df_all.astype({'date':'datetime64[ns]', 'isin':'str'})
for col in X_FEATURES:
      df_all[col] = df_all[col].astype('float32')

# sort by columns for latter grouping indices
df_all = df_all.sort_values(['isin', 'date']).dropna().reset_index(drop=True)
indices_dict = df_all.iloc[:100000].groupby('isin').indices
np_all = df_all[X_FEATURES +  Y_FEATURES].iloc[:100000].to_numpy()
win_len = 40
test_size = 80
val_size = 80
X_train, y_train, X_val, y_val, X_test, y_test, train_indices, val_indices, test_indices = get_input_arrays(indices_dict, np_all, win_len, test_size, val_size)
print(X_train.shape, y_train.shape, X_val.shape, 
        y_val.shape, X_test.shape, y_test.shape)
ADD_CAT_BASE = np.abs(np.min(np.min(y_train), 0))
y_train_cat = tf.keras.utils.to_categorical(y_train + ADD_CAT_BASE, 4)
y_val_cat = tf.keras.utils.to_categorical(y_val + ADD_CAT_BASE, 4)
y_test_cat = tf.keras.utils.to_categorical(y_test + ADD_CAT_BASE, 4)

dataset_train = tf.data.Dataset.from_tensor_slices((X_train, y_train_cat))
dataset_train = dataset_train.batch(N_BATCH_SIZE).prefetch(tf.data.AUTOTUNE)

dataset_val = tf.data.Dataset.from_tensor_slices((X_val, y_val_cat))
dataset_val = dataset_val.batch(N_BATCH_SIZE).prefetch(tf.data.AUTOTUNE)

dataset_test = tf.data.Dataset.from_tensor_slices((X_test, y_test_cat))
dataset_test = dataset_test.batch(N_BATCH_SIZE).prefetch(tf.data.AUTOTUNE)


# with open(PATH_DATA + FILE_PICKLE, 'rb') as f:
#     X_train_org = pickle.load(f)
#     y_train_org = pickle.load(f)
#     X_test_org = pickle.load(f)
#     y_test_org = pickle.load(f)
# print(X_train_org.shape, y_train_org.shape, X_test_org.shape, y_test_org.shape)

# EXCEPTIONAL: CATEGORICAL STRANGE BEHAVIOR WITH NEGATIVE VALUES --> 0 INDEX ARRAY:

# categorical model
with tf.keras.device('/GPU:0'): #device:
  K.clear_session()
  print(tf.config.list_physical_devices("GPU"))

  lstm_model = Sequential(
      [   # 1st LSTM layer
          tf.keras.layers.Bidirectional(
            tf.keras.layers.LSTM(
                units=48,
                return_sequences=True # gives back a y-predict for each timestep (needed for the input of LSTM-layer 2)
                ),
                input_shape=(SEQUENCE_LENGTH, X_train.shape[2])
          ),
          tf.keras.layers.Dropout(0.3),
          # 2nd LSTM layer
          tf.keras.layers.Bidirectional(
            tf.keras.layers.LSTM(units=32,
                return_sequences=True)
          ),
          tf.keras.layers.Dropout(0.3),
          #  3rd LSTM layer
          tf.keras.layers.Bidirectional(
            tf.keras.layers.LSTM(units=24)
          ),
          # 1st dense layer
          # keras.layers.Dropout(0.2),
          # keras.layers.Dense(units=16, activation='relu'),
          # Output Layer
          tf.keras.layers.Dropout(0.3),
          tf.keras.layers.Dense(units=4, activation='softmax')
      ]
  )
  lstm_model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy', Precision(), Recall()])
  lstm_model.summary()

  # fit the network
  history = lstm_model.fit(
              dataset_train, #X_train, y_train_cat
              epochs=N_EPOCHS,
              # batch_size=N_BATCH_SIZE,
              # validation_split=0.1,
              validation_data=dataset_val,
              class_weight=CLASS_WEIGHTS, # to balance the unbalanced data (put more weight on class 1)
              verbose=1, # defines if animation will be shown while training
              callbacks = [EarlyStopping(monitor='val_accuracy', min_delta=0.0001, patience=0, verbose=1, mode='auto')]
              )

  # evaluate the model and print the results
  score = lstm_model.evaluate(dataset_test, verbose=0) # X_test, y_test_cat
  print(score)
  print("Test loss:", score[0], "\nTest accuracy:", score[1], 
        "\nTest precision:", score[2], "\nTest recall:", score[3])
  
  # create confusion map for test
  y_pred_test_cat = lstm_model.predict(dataset_test) # X_test
  y_pred_test = np.argmax(y_pred_test_cat, axis=1) - ADD_CAT_BASE
  cm = confusion_matrix(y_test, y_pred_test)
  print(cm)

  #sns.heatmap(cm, annot=True, fmt="d")
  df_temp = pd.DataFrame({'timestamp':datetime.datetime.now(),'sequence_length':SEQUENCE_LENGTH,
                          'N_EPOCHS':N_EPOCHS,'N_BATCH_SIZE':N_BATCH_SIZE, 'LSTM_1_UNITS':LSTM_1_UNITS,'LSTM_2_UNITS':LSTM_2_UNITS,
                          'LSTM_3_UNITS':LSTM_3_UNITS,'class_weights':str(list(CLASS_WEIGHTS.items())),
                          'Test_loss':score[0],'Test accuracy':score[1],'Test precision':score[2],'Test recall':score[3],
                          'cm_row0':str(cm[0]),'cm_row1':str(cm[1]),'cm_row2':str(cm[2]), 'cm_row3':str(cm[3])}, # ,'cm_row4':str(cm[4])
                          index=[0])

  df_temp.to_csv("results.csv", index=False)

df_test = pd.DataFrame(test_indices).melt(var_name='symbol', value_name='index')
df_test['y_true'] = y_test
df_test['y_hat'] = y_pred_test
df_test['y_dist'] = y_pred_test - y_test
df_test.to_csv(PATH_DATA + "test_results.csv", index=False)

# # save data to pickle file
# FILE_PICKLE = "bi_lstm_model.pickle"

# with open(PATH_DATA + FILE_PICKLE, 'wb') as f:
#     # Pickle the 'data' dictionary using the highest protocol available.
#     # pickle.dump(X_train, f, pickle.HIGHEST_PROTOCOL)
#     # pickle.dump(y_train, f, pickle.HIGHEST_PROTOCOL)
#     # pickle.dump(X_test, f, pickle.HIGHEST_PROTOCOL)
#     # pickle.dump(y_test, f, pickle.HIGHEST_PROTOCOL)
#     # pickle.dump(train_indices, f, pickle.HIGHEST_PROTOCOL)
#     # pickle.dump(test_indices, f, pickle.HIGHEST_PROTOCOL)
#     pickle.dump(lstm_model, f, pickle.HIGHEST_PROTOCOL)
#     # pickle.dump(df_test, f, pickle.HIGHEST_PROTOCOL)