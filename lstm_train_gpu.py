import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns
import numpy as np
import datetime
import os
import pickle

from sklearn.preprocessing import MinMaxScaler, PolynomialFeatures, StandardScaler
from sklearn.metrics import confusion_matrix, accuracy_score
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, roc_curve

import tensorflow as tf
#f rom tensorflow import keras, initializers, device
# from tensorflow.keras.utils import to_categorical
from keras.models import Sequential
# from keras.layers import Dense, Dropout, LSTM, Activation, Bidirectional
from keras.callbacks import EarlyStopping
from keras import backend as K
from keras.metrics import Precision, Recall, CategoricalAccuracy
# import io

BATCH_SIZE = 64 # optimized 48
WIN_LEN = 30
TRAIN_RATIO = 0.75
VAL_RATIO = 0.125
TEST_RATIO = 0.125
FLAT_CLASS = 1

N_EPOCHS = 20 # optimized [16, 18, 20]
CLASS_WEIGHTS = {0:2, 1:1, 2:2} # oprtimized [{0: 1, 1: 2, 2: 2}, {0: 1, 1: 2, 2: 3}, {0: 1, 1: 3, 2: 4}, {0: 1, 1: 4, 2: 4}]
LSTM_1_UNITS = 64 # [48, 64, 80] # optimum: (DATASET 1: 32) (DATASET 2: 64)
LSTM_2_UNITS = 48 # [40, 48, 56] # optimum: (DATASET 1: 24) (DATASET 2: 48)
LSTM_3_UNITS = 32 # [24, 32, 40] # optimum: (DATASET 1: 24) (DATASET 2: 32)

# PATH = './results/'
PATH_DATA = "./data_lstm/"
# PATH_DATA = "./data/"
FILE_DATA = "data_lstm_ind_2_classes_q175.csv" # 'data_lstm_ind_4_classes.csv'

X_FEATURES = ['ret_1', 'ret_5', 'ret_20', 'ret_60', 'vola_20', 'vola_ratio', 
              'volume_ratio', 'volume_5', 'volume_20', 'volume_trend', 'hi_lo_ratio',
                'close_pos', 'market_ret_1', 'market_vola', 'beta', 'rank_ret_20',
              'RSI', 'ma_ratio_5', 'ma_ratio_10', 'ma_ratio_20'] 
              # 'z_score_ret_1', 'z_score_ret_5', 'z_vol', 'z-volatility', 'div_norm', 
              #'dax_ret_1', 'dax_ret_5', 'dax_ma_ratio_20', 'dax_z-volatility', 
               # 'msci_ret_1', 'msci_ret_5', 'msci_ma_ratio_20', 'msci_z-volatility',
               # 'dax_ret_1_cross', 'dax_ret_5_cross', 'msci_ret_1_cross', 'msci_ret_5_cross']
Y_FEATURES = ['class']

print(tf.config.list_physical_devices("GPU"))

def count_classes(y_true):
    return y_true.shape[0] - np.sum(y_true) , np.sum(y_true) 

def build_datasets_interleave(x, y, isin, win_len, batch_size, train_ratio=0.7, val_ratio=0.15, test_ratio=0.15):
    """
    Efficiently build streaming train, val, test datasets for many ISINs.
    Uses interleave to avoid huge concatenate chains.
    """

    NUM_CLASSES = len(np.unique(y))
    unique_isin = np.unique(isin)

    def make_isin_ds(u):
        # slices x and y into isin part
        idx = np.where(isin == u)[0]
        data_x = x[idx]
        data_y = y[idx]
        n = len(data_x)

        if n <= win_len:
            return tf.data.Dataset.from_tensors((tf.zeros([0, win_len, x.shape[1]]),
                                                tf.zeros([0], dtype=tf.int32))).skip(1)

        # compute split indices
        n_windows = n - win_len + 1
        n_train = int(n_windows * train_ratio)
        n_val   = int(n_windows * val_ratio)

        # create dataset
        ds = tf.keras.utils.timeseries_dataset_from_array(
            data=data_x,
            targets=data_y[win_len - 1:],  # predict next step
            sequence_length=win_len,
            sequence_stride=1,
            shuffle=False,  # shuffle later
            batch_size=None # first train test split
        )

        # split
        train_ds = ds.take(n_train)
        val_ds   = ds.skip(n_train).take(n_val)
        test_ds  = ds.skip(n_train + n_val)

        return train_ds, val_ds, test_ds

    # interleave to avoid long concatenation chain
    def interleave_datasets(datasets, cycle_length=32):
        ds = tf.data.Dataset.from_tensor_slices(datasets)
        ds = ds.interleave(
            lambda d: d,
            cycle_length=cycle_length,
            block_length=1,
            num_parallel_calls=tf.data.AUTOTUNE
        )
        return ds
        
    # collect datasets per ISIN
    train_list, val_list, test_list = [], [], []
    for u in unique_isin:
        tr, va, te = make_isin_ds(u)
        train_list.append(tr)
        val_list.append(va)
        test_list.append(te)

    train_ds = interleave_datasets(train_list)
    val_ds   = interleave_datasets(val_list)
    test_ds  = interleave_datasets(test_list)

    # train_ds = tf.data.Dataset.from_tensor_slices(train_list)
    # val_ds   = tf.data.Dataset.from_tensor_slices(val_list)
    # test_ds  = tf.data.Dataset.from_tensor_slices(test_list)

    # apply one-hot, shuffle, batch, prefetch
    def one_hot_y(x, y):
        y = tf.one_hot(tf.cast(y, tf.int32), depth=NUM_CLASSES)
        return x, y
    
    # apply one-hot, shuffle, batch, prefetch
    def filter_flat_class(x, y):
        return tf.not_equal(y, FLAT_CLASS)

    # train_ds = train_ds.map(one_hot_y, num_parallel_calls=tf.data.AUTOTUNE)
    train_ds = train_ds.map(one_hot_y).shuffle(2000).batch(batch_size).prefetch(tf.data.AUTOTUNE) # .filter(filter_flat_class)
    val_ds = val_ds.map(one_hot_y).batch(batch_size).prefetch(tf.data.AUTOTUNE) # .filter(filter_flat_class).map(one_hot_y)
    test_ds = test_ds.map(one_hot_y).batch(batch_size).prefetch(tf.data.AUTOTUNE) # .filter(filter_flat_class).map(one_hot_y)

    return train_ds, val_ds, test_ds

# load data
df_all = pd.read_csv(PATH_DATA + FILE_DATA)
# cast dtypes to save RAM
df_all = df_all.astype({'date':'datetime64[ns]', 'isin':'str'})
# sort by columns for latter grouping indices
len_before = df_all.shape[0]
df_all = df_all.sort_values(['isin', 'date']).dropna().reset_index(drop=True)
print(df_all.shape[0] - len_before, "na dropped")
# scale data
scaler = StandardScaler()
df_all[X_FEATURES] = scaler.fit_transform(df_all[X_FEATURES])
# create numpy array
isin_groups = df_all.groupby("isin").size()
len_test = int(df_all.shape[0])
X = df_all[X_FEATURES].iloc[:len_test].to_numpy(dtype="float32")
y = df_all[Y_FEATURES].iloc[:len_test].to_numpy(dtype="int32").squeeze()
isin = df_all["isin"].iloc[:len_test].to_numpy()
print("data loaded from disc")

# create datasets ((B, S, F), (B, S))
train_ds, val_ds, test_ds = build_datasets_interleave(
    X, y, isin,
    win_len=WIN_LEN,
    batch_size=BATCH_SIZE,
    train_ratio=TRAIN_RATIO,
    val_ratio=VAL_RATIO
)
print("data streaming initialized")
    
# # delete df_all
# del df_all
# df_all = None

# categorical model
with tf.keras.device('/GPU:0'): #device:
  K.clear_session()
  print(tf.config.list_physical_devices("GPU"))
  lstm_model = Sequential(
      [   # 1st LSTM layer
        tf.keras.Input(shape=(WIN_LEN, X.shape[1])), # (w, f)
        tf.keras.layers.LSTM(
            units=16,
            return_sequences=False, # hidden states per t as input of next LSTM-layer
            kernel_regularizer=tf.keras.regularizers.l2(0.01),
            recurrent_regularizer=tf.keras.regularizers.l2(0.01)
            ),
        tf.keras.layers.Dropout(0.3),
          # 1st dense layer
        # tf.keras.layers.Dense(units=16, activation='relu'),
          # Output Layer
        tf.keras.layers.Dense(units=3, activation='softmax') # 'softmax' 'sigmoid'
        # tf.keras.layers.Dense(units=1, activation='sigmoid') # 'softmax' 'sigmoid'
      ]
  )

#   lstm_model = Sequential(
#       [   # 1st LSTM layer
#           tf.keras.Input(shape=(WIN_LEN, X.shape[1])),
#           tf.keras.layers.Bidirectional(
#             tf.keras.layers.LSTM(
#                 units=48,
#                 return_sequences=True # gives back a y-predict for each timestep (needed for the input of LSTM-layer 2)
#                 )
#           ),
#           tf.keras.layers.Dropout(0.3),
#           # 2nd LSTM layer
#           tf.keras.layers.Bidirectional(
#             tf.keras.layers.LSTM(units=32,
#                 return_sequences=True)
#           ),
#           tf.keras.layers.Dropout(0.3),
#           #  3rd LSTM layer
#           tf.keras.layers.Bidirectional(
#             tf.keras.layers.LSTM(units=24)
#           ),
#           # 1st dense layer
#           # keras.layers.Dropout(0.2),
#           # keras.layers.Dense(units=16, activation='relu'),
#           # Output Layer
#           tf.keras.layers.Dropout(0.3),
#           tf.keras.layers.Dense(units=4, activation='softmax')
#       ]
#   )
  lstm_model.compile(loss='categorical_crossentropy', 
                     # loss="sparse_categorical_crossentropy", # 'BinaryCrossentropy', #"sparse_categorical_crossentropy", # 
                     # loss="BinaryCrossentropy", # 'BinaryCrossentropy', #"sparse_categorical_crossentropy", # 
                     optimizer=tf.keras.optimizers.Adam(learning_rate=3e-4),
                     # metrics=['accuracy', Precision(), Recall()]) # , CategoricalAccuracy(name="cat_acc") , CategoricalAccuracy(name="cat_acc"), Precision(), Recall()
                     metrics = ["accuracy", 
                                tf.keras.metrics.AUC(multi_label=True, num_labels=3), 
                                tf.keras.metrics.Precision(), 
                                tf.keras.metrics.Recall()])
  lstm_model.summary()
  print("model build")

  # one_batch = train_ds.take(1000)

  # fit the network
  history = lstm_model.fit(
              train_ds, #train_ds
              epochs=N_EPOCHS,
              validation_data=val_ds,
              class_weight=CLASS_WEIGHTS, # to balance the unbalanced data (put more weight on class 1)
              verbose=1, # defines if animation will be shown while training
              # callbacks = [EarlyStopping(monitor='val_accuracy', min_delta=0.0001, patience=0, verbose=1, mode='auto')]
              callbacks = EarlyStopping(monitor='val_loss', 
                                        patience=5, 
                                        restore_best_weights=True,
                                        min_delta=1e-4),
              # steps_per_epoch=train_steps
            # batch_size=N_BATCH_SIZE,
              # validation_split=0.1,
              )
  print("model train finished")
  
  # evaluate the model and print the results
  score = lstm_model.evaluate(test_ds, verbose=1) # X_test, y_test_cat
  print(score)
  print("Test loss:", score[0], "\nTest accuracy:", score[1]) #, 
        # "\nTest cat accr.:", score[2])
  

  THRES = 0.75
  # create confusion map for test
  y_pred_train = lstm_model.predict(train_ds) # X_test
  # y_pred_train_class = (y_pred_train + 0.5).astype(int) 
  # y_true_train = np.concatenate([y.numpy() for _, y in train_ds])
  y_pred_train_class = np.argmax(y_pred_train, axis=1)
  y_true_train = np.argmax(np.concatenate([y.numpy() for _, y in train_ds]), axis=1)
  cm = confusion_matrix(y_true_train, y_pred_train_class)
  # up, down = count_classes(y_true_train)
  # print(f"train cm: up {up}, down {down}\n", cm)
  print(f"train cm:\n", cm)
  # print cm for sure predictions
  mask = (y_pred_train > THRES) | (y_pred_train < 1 - THRES)
  up, down = count_classes(y_true_train[mask.reshape(-1)])
  cm = confusion_matrix(y_true_train[mask.reshape(-1)], y_pred_train_class[mask])
  print(f"top train cm: up {up}, down {down}\n", cm)
  
# create confusion map for validation
  y_pred_val = lstm_model.predict(val_ds) # X_test
  # y_pred_val_class = (y_pred_val + 0.5).astype(int) 
  # y_true_val = np.concatenate([y.numpy() for _, y in val_ds])
  y_pred_val_class = np.argmax(y_pred_val, axis=1) 
  y_true_val = np.argmax(np.concatenate([y.numpy() for _, y in val_ds]), axis=1)
  cm = confusion_matrix(y_true_val, y_pred_val_class)
  # up, down = count_classes(y_true_val)
  # print(f"val cm: up {up}, down {down}\n", cm)
  print(f"val cm:\n", cm)

# create confusion map for test
  y_pred_test = lstm_model.predict(test_ds) # X_test
  # y_pred_test_class = (y_pred_test + 0.5).astype(int) 
  # y_true_test = np.concatenate([y.numpy() for _, y in test_ds])
  y_pred_test_class = np.argmax(y_pred_test, axis=1) 
  y_true_test = np.argmax(np.concatenate([y.numpy() for _, y in test_ds]), axis=1)
  cm = confusion_matrix(y_true_test, y_pred_test_class)
  # up, down = count_classes(y_true_test)
  # print(f"test cm: up {up}, down {down}\n", cm)
  print(f"test cm:\n", cm)



  # print cm for sure predictions
  mask = (y_pred_val > THRES) | (y_pred_val < 1 - THRES)
  cm = confusion_matrix(y_true_val[mask.reshape(-1)], y_pred_val_class[mask])
  up, down = count_classes(y_true_val[mask.reshape(-1)])
  print(f"top val cm: up {up}, down {down}\n", cm)  
  fpr, tpr, thresholds = roc_curve(y_true_val, y_pred_val)
  best_threshold = thresholds[np.argmax(tpr - fpr)]
  mask = (y_pred_val > best_threshold) | (y_pred_val < 1 - best_threshold)
  cm = confusion_matrix(y_true_val[mask.reshape(-1)], y_pred_val_class[mask])
  up, down = count_classes(y_true_val[mask.reshape(-1)])
  print(f"best val cm: up {up}, down {down}\n", cm)  


 
  # create confusion map for test
  y_true_test = np.concatenate([y.numpy() for _, y in test_ds])
  y_pred_test = lstm_model.predict(test_ds) # X_test
  # y_pred_test_class = np.argmax(y_pred_test, axis=1)
  y_pred_test_class = (y_pred_test + 0.5).astype(int)  
  cm = confusion_matrix(y_true_test, y_pred_test_class)
  print(cm)

  # print cm for sure predictions
  mask = (y_pred_test > THRES) | (y_pred_test < 1 - THRES)
  cm = confusion_matrix(y_true_test[mask.reshape(-1)], y_pred_test_class[mask])
  print('top test\n', cm)
  
  print("AUC train:", roc_auc_score(y_true_train, y_pred_train))
  print("AUC val:", roc_auc_score(y_true_val, y_pred_val))
  print("AUC test:", roc_auc_score(y_true_test, y_pred_test))

  #sns.heatmap(cm, annot=True, fmt="d")
  df_temp = pd.DataFrame({'timestamp':datetime.datetime.now(),'sequence_length':WIN_LEN,
                          'N_EPOCHS':N_EPOCHS,'N_BATCH_SIZE':BATCH_SIZE, 'LSTM_1_UNITS':LSTM_1_UNITS,'LSTM_2_UNITS':LSTM_2_UNITS,
                          'LSTM_3_UNITS':LSTM_3_UNITS,'class_weights':str(list(CLASS_WEIGHTS.items())),
                          'Test_loss':score[0],'Test accuracy':score[1],
                          'cm_row0':str(cm[0]),'cm_row1':str(cm[1])}, # ,'cm_row4':str(cm[4])
                          index=[0])

  df_temp.to_csv("results.csv", index=False)

# df_test = pd.DataFrame(test_indices).melt(var_name='symbol', value_name='index')
# df_test['y_true'] = y_true
# df_test['y_hat'] = y_pred_test
# df_test['y_dist'] = y_pred_test - y_test
# df_test.to_csv(PATH_DATA + "test_results.csv", index=False)

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
print("code sucessfully finished")


# # logistic regression model --------------------------------------------------
def collect_sample(ds, n_batches=200):
    X_list, y_list = [], []
    for i, (x, y) in enumerate(ds.take(n_batches)):
        X_list.append(x.numpy())
        y_list.append(y.numpy())
    X = np.concatenate(X_list)
    y = np.concatenate(y_list)
    return X, y

X_win, y_win = collect_sample(train_ds, n_batches=200)
X_win_val, y_win_val = collect_sample(val_ds, n_batches=200)
# X_last = X_win[:, -1, :]


# clf = LogisticRegression(max_iter=500)
# clf.fit(X_last, y_win)

# proba = clf.predict_proba(X_last)[:, 1]
# print("AUC last step:", roc_auc_score(y_win, proba))

# flatted window
X_flat = X_win.reshape(X_win.shape[0], -1)
X_flat_val = X_win.reshape(X_win_val.shape[0], -1)
clf = LogisticRegression(max_iter=1000)
clf.fit(X_flat, y_win)
proba = clf.predict_proba(X_flat_val)[:, 1]
print("AUC last step:", roc_auc_score(y_win_val, proba))

# # trend structure
# X_feat = np.concatenate([
#     X_win.mean(axis=1),
#     X_win.std(axis=1),
#     X_win[:, -1, :] - X_win[:, 0, :],
# ], axis=1)

# clf = LogisticRegression(max_iter=500)
# clf.fit(X_feat, y_win)

# proba = clf.predict_proba(X_feat)[:, 1]
# print("AUC simple temporal features:", roc_auc_score(y_win, proba))
