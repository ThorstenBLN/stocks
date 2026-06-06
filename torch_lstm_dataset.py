import pandas as pd
import numpy as np
import datetime as dt
import torch
from torch.utils.data import Dataset
import os
from typing import Literal

class StocksDataset(Dataset):
# class StocksDataset():
    def __init__(self, path_data:str, file_data:str, len_seq:int, features:list, target:list, n_classes:int, mode:Literal["train", "val", "test"]='train', splits:tuple=(0.7, 0.15, 0.15)):
        self.len_seq = len_seq
        self.features = features
        self.target = target
        self.n_classes = n_classes
        # load data 
        self.df_all = pd.read_csv(os.path.join(path_data, file_data), parse_dates=['date']).sort_values(['isin', 'date']).reset_index(drop=True)
        self.df_all['date'] = self.df_all['date'].dt.date
        # define train test split dates
        total_days = (self.df_all['date'].max() - self.df_all['date'].min()).days - len_seq
        self.val_date = self.df_all['date'].min() + dt.timedelta(days=total_days * (1 - splits[1] - splits[2]))
        self.test_date = self.df_all['date'].min() + dt.timedelta(days=total_days * (1 - splits[2]))
        # create list with relevant indices (start, end, start val and test data)
        self.df_indices = self.df_all.groupby(['isin']).agg(start_data=('date', lambda x: x.index.min()),
                            end_data=('date', lambda x: x.index.max())).reset_index()
        df_val_index = self.df_all.loc[self.df_all['date'] >= self.val_date].groupby('isin')['date'].apply(lambda x: x.index.min()).reset_index().rename(columns={'date':'val_start'})
        df_test_index = self.df_all.loc[self.df_all['date'] >= self.test_date].groupby('isin')['date'].apply(lambda x: x.index.min()).reset_index().rename(columns={'date':'test_start'})
        self.df_indices = self.df_indices.merge(df_val_index, how='left', on='isin').merge(df_test_index, how='left', on='isin')
        self.df_indices['val_start'] = np.where(self.df_indices['val_start'].isna(), self.df_indices['end_data'], self.df_indices['val_start'])
        self.df_indices['test_start'] = np.where(self.df_indices['test_start'].isna(), self.df_indices['end_data'], self.df_indices['test_start'])

        # create an array with all start and end indexs (creation: separate for each isin)
        indices_list = []
        for row in self.df_indices.iloc[:].itertuples():
            if mode == 'train':
                start = row.start_data
                end = row.val_start
            elif mode == 'val':
                start = row.val_start
                end = row.test_start
            else:
                start = row.test_start
                end = row.end_data + 1
            # create indices for each window
            isin_dates_array = np.zeros(shape=(end - start, 3))
            isin_dates_array[:, 0] = row.Index
            isin_dates_array[:, 1] = np.arange(start, end)
            isin_dates_array[:, 2] = isin_dates_array[:, 1] + len_seq
            # cut off invalid data
            isin_dates_array = np.int32(isin_dates_array[isin_dates_array[:, 2] <= row.end_data + 1])
            indices_list.append(isin_dates_array)
        self.indices_np = np.concatenate(indices_list, dtype=np.int32) # contains always 1 more as will be cut of by array slicing

        # convert data to numpy
        self.data = self.df_all[self.features + self.target].to_numpy(dtype=np.float32)
        # status update

        print(f"""{mode} dataset created. all data: {self.df_all['date'].min()} - {self.df_all['date'].max()}
                                               Val: {self.val_date} Test: {self.test_date}""")

    def __len__(self):
        return self.indices_np.shape[0]
    
    def __getitem__(self, idx):
        start = self.indices_np[idx, 1]
        end = self.indices_np[idx, 2]
        x = self.data[start:end, :-1]
        y = self.data[end - 1, -1].astype(np.int32)
        return x, y
