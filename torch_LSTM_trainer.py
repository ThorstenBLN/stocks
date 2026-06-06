import torch
from torch import nn, optim
import torch_lstm_dataset
import torch_LSTM_model
from torch.utils.data import DataLoader
import time
import numpy as np
from tqdm import tqdm
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay 
import matplotlib.pyplot as plt


PATH_DATA = "./data_lstm/"
FILE_DATA = "data_lstm_ind_2_classes_q175.csv" 
X_FEATURES = ['ret_1', 'ret_5', 'ret_20', 'ret_60', 'vola_20', 'vola_ratio', 
              'volume_ratio', 'volume_trend', 'hi_lo_ratio',
                'close_pos', 'market_ret_1', 'market_vola', 'beta', 'rank_ret_20',
              'RSI', 'ma_ratio_5', 'ma_ratio_10', 'ma_ratio_20'] #, 'volume_5', 'volume_20'
Y_FEATURES = ['class']
splits=(0.8, 0.10, 0.10)
mode = 'train'
n_classes = 3
win_len = 40
hidden_size = 24
batch_size = 64
n_epochs = 15
learning_rate = 5e-4
cuda = False
clip_grad = 2
dropout_p = 0.3
EPS = 5e-5

def plot_confusion_matrix(y_true, y_pred):
    cm = confusion_matrix(y_true, y_pred)
    disp = ConfusionMatrixDisplay(confusion_matrix=cm)
    disp.plot()
    plt.show()


# start the training run
def train(model, loss_func, optimizer, n_epochs, device, train_dataloader, val_dataloader):
    train_time = begin_time = time.time()
    print('Min log loss training')

    for epoch in range(n_epochs):
        y_train_true, y_train_pred =  train_loop(model, loss_func, optimizer, train_dataloader, device, epoch, n_epochs)
        # print(epoch, ". epoch_loss:", np.round(loss/train_dataloader.__len__(), 5))
        y_val_true, y_val_pred = val_loop(model, val_dataloader, device, n_classes, epoch, n_epochs)
    plot_confusion_matrix(y_train_true, y_train_pred)
    plot_confusion_matrix(y_val_true, y_val_pred)
    
def train_loop(model, loss_func, optimizer, train_dataloader, device, epoch, n_epochs):
    model.train()
    loss_epoch = 0
    correct = 0
    total = 0
    y_pred_all = []
    y_true_all = []
    loop = tqdm(train_dataloader, desc=f"Epoch [{epoch+1}/{n_epochs}]")
    for i, (x, y) in enumerate(loop):
        # 0 set all to zero
        optimizer.zero_grad()
        loss = 0
        # 1. convert numpy to pytorch
        train_x = x.float().to(device)
        train_y = y.long().to(device)
        # print(x.max(axis= 2))
        # print("train_x std:", train_x.std().item())
        # 1. Forward
        logits = model(train_x)
        y_pred = torch.argmax(logits, dim=1)
        # 2. Loss
        loss = loss_func(logits, train_y)
        # print(torch.argmax(logits, dim=1))
        # 3. Backward
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), clip_grad)
        # 4. Update
        optimizer.step()
        # # 5. update loss metric
        loss_epoch += loss.item()
        correct += (train_y == y_pred).sum().item()
        total += y_pred.size(0)

        avg_loss = loss_epoch / (i + 1)
        acc = correct / total

        metrics = {
            'loss': f"{avg_loss:.4f}",
            'acc': f"{acc:.4f}"
        }
        loop.set_postfix(metrics)

        y_pred_all.append(y_pred)
        y_true_all.append(train_y)
        if i == len(train_dataloader) - 1:
            for i, p in enumerate(lstm_model.parameters()):
                if i in [2, 3]:
                    print(p.shape, np.round(p.grad.mean().item(), 5), np.round(p.grad.std().item(), 5), np.round(p.grad.max().item(), 5))
        # if i % 20 == 0: 
    return torch.cat(y_true_all, 0),  torch.cat(y_pred_all, 0)

def val_loop(model, val_dataloader, device, n_classes, epoch, n_epochs):
    model.eval()
    loss_epoch = 0
    correct = 0
    total = 0
    y_pred_all = []
    y_true_all = []
    pr_rc_data = {cl:[0, 0, 0] for cl in range(n_classes)}
    loop = tqdm(val_dataloader, desc=f"E-Val [{epoch+1}/{n_epochs}]")
    with torch.no_grad():
        for i, (x, y) in enumerate(loop):
            # 0. convert numpy to pytorch
            val_x = x.float().to(device)
            val_y = y.long().to(device)
            # 1. Forward
            logits = model(val_x)
            y_pred = torch.argmax(logits, dim=1)
            # 2. calculate loss
            loss = loss_func(logits, val_y)
            # 3. calculate metrics
            loss_epoch += loss.item()
            correct += (val_y == y_pred).sum().item()
            total += y_pred.size(0)
            avg_loss = loss_epoch / (i + 1)
            acc = correct / total
            y_pred_all.append(y_pred)
            y_true_all.append(val_y)
            # 4. update precision and recall kpi
            for cl in range(n_classes):
                pr_rc_data[cl][0] += ((val_y == cl) & (y_pred == cl)).sum().item() # TP
                pr_rc_data[cl][1] += ((val_y != cl) & (y_pred == cl)).sum().item() # FP
                pr_rc_data[cl][2] += ((val_y == cl) & (y_pred != cl)).sum().item() # FN
            prec = np.mean([value[0] / np.max((value[0] + value[1], EPS)) for _, value in pr_rc_data.items()]) # TP / (TP + FP)
            rec = np.mean([value[0] / np.max((value[0] + value[2], EPS)) for _, value in pr_rc_data.items()]) # TP / (TP + FN)
            metrics = {
                'val_loss': f"{avg_loss:.4f}",
                'val_acc': f"{acc:.4f}",
                'val_prec': f"{prec:.4f}",
                'val_recall': f"{rec:.4f}"
                }
            loop.set_postfix(metrics)    
    return torch.cat(y_true_all, 0),  torch.cat(y_pred_all, 0)

# 0. define device
device = torch.device("cuda:0" if cuda else "cpu")
print('use device: %s' % device)

# 1. instatiate the model
lstm_model = torch_LSTM_model.LstmClassifier(device, n_features=len(X_FEATURES), win_len=win_len, 
                                             n_classes=3, hidden_size=hidden_size, dropout_prob = dropout_p )
# lstm_model =  torch_LSTM_model.LstmMultiheadAttentionClassifier(n_features=len(X_FEATURES), 
#                                                  win_len=win_len,
#                                                  hidden_size=16, 
#                                                  heads=4,
#                                                  att_rank=4 * 4, 
#                                                  n_classes=n_classes, 
#                                                  dropout_prob=0.3)
lstm_model = lstm_model.to(device)
# 2. instantiate adam optimizer
optimizer = optim.Adam(lstm_model.parameters(), lr=learning_rate, weight_decay=1e-4)
# 3. instantiate loss function (negative log loss as multiple class problem)
loss_func = torch.nn.NLLLoss(weight=torch.tensor([2.3, 1., 1.9])) # weight=torch.tensor([2, 1, 2])
# 4. instantiate dataloaders
print("initializing train dataloader")
train_dataset = torch_lstm_dataset.StocksDataset(PATH_DATA, FILE_DATA, win_len, X_FEATURES, 
                                                Y_FEATURES, n_classes, mode='train', splits=splits)
train_dataloader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
print("initializing val dataloader")
val_dataset = torch_lstm_dataset.StocksDataset(PATH_DATA, FILE_DATA, win_len, X_FEATURES, 
                                            Y_FEATURES, n_classes, mode='val', splits=splits)
val_dataloader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False, drop_last=True)
# 5. train model
train(lstm_model, loss_func, optimizer, n_epochs, device, train_dataloader, val_dataloader)
