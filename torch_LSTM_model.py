import torch
import torch.nn as nn
import torch.nn.functional as F
import math

class LstmClassifier(nn.Module):
    def __init__(self, device, n_features:int=36, win_len:int=40,
        hidden_size=16, n_classes=3, dropout_prob=0.3):
        super(LstmClassifier, self).__init__()

        self.n_features = n_features
        self.win_len = win_len
        self.hidden_size = hidden_size
        self.n_classes = n_classes
        self.dropout_prob = dropout_prob

        # define parameters
        self.output_weight = nn.Parameter(torch.ones(self.hidden_size, self.n_classes), requires_grad=True) # (h, c)
        nn.init.xavier_uniform_(self.output_weight)
        self.output_bias = nn.Parameter(torch.ones(self.n_classes), requires_grad=True) # (h)
        nn.init.uniform_(self.output_bias)

        # define the layers of the model
        self.lstm_1 = nn.LSTM(self.n_features, self.hidden_size, batch_first=True, device=device) # output (1, h)
        self.log_softmax = nn.LogSoftmax(dim=1)
        
    def forward(self, x):
        # print("x initial:", x.shape)
        # x = torch.permute(x, (1, 0, 2))
        # print("x permute:", x.shape)
        h_all, (h_last, c_last) = self.lstm_1(x) #B, S, L
        # print("h_all std:", h_all.std().item())
        # print("h_last shape:", h_last.shape)
        # print("h_all shape:", h_all.shape)
        x_h_last = h_all[:, -1, :] # (B, L)
        # print(f"x_h_kast: {x_h_last.mean():.4f}, {x_h_last.std():.4f}, {x_h_last.min():.4f}, {x_h_last.max():.4f}")
        # print("x_h_last_shape:", x_h_last.shape)
        # print("self.output_weight:", self.output_weight.shape)
        # print("self.output_bias:", self.output_bias.shape)
        self.dropout = nn.Dropout(p=self.dropout_prob)
        x_1 = x_h_last @ self.output_weight + self.output_bias  # (B, L) @ (L, C) + C = (B, C)
        # print("x_1_shape:", x_1.shape)
        # x_1 = h_last @ self.output_weight + self.output_bias  # 1xbxh @ hxc + c = 1xbxc
        # print("x_out shape:", x_1.shape)
        # x_1 = x_1.squeeze(1) # (B, C)
        # print("x_out reshaped:", x_1.shape)
        y = self.log_softmax(x_1) # bxc
        # print("y shape:", y.shape)
        return y


class TransformerClassifier(nn.Module):
    def __init__(self, n_features:int=36, hidden_size:int=16,
                 dim_key:int=24, n_classes:int=3, dropout_prob:float=0.3):
        super(LstmClassifier, self).__init__()

        self.n_features = n_features
        self.hidden_size = hidden_size
        self.dim_key = dim_key
        self.n_classes = n_classes
        self.dropout_prob = dropout_prob

        # define parameters
        self.output_weight = nn.Parameter(torch.ones(self.hidden_size, self.n_classes), requires_grad=True) # (h, c)
        nn.init.xavier_uniform_(self.output_weight)
        self.output_bias = nn.Parameter(torch.ones(self.n_classes), requires_grad=True) # (h)
        nn.init.uniform_(self.output_bias)

        # define the layers of the model
        self.lstm_1 = nn.LSTM(self.n_features, self.hidden_size, dropout=self.dropout_prob, batch_first=True) # output (1, h)
        self.log_softmax = nn.LogSoftmax(dim=1)
        # TODO
        # define attention block 
        self.query_weigths = nn.Parameter(torch.ones(self.hidden_size, self.dim_key), requires_grad=True) # (h, d)
        self.key_weigths = nn.Parameter(torch.ones(self.hidden_size, self.dim_key), requires_grad=True) # (h, d)
        self.att_softmax = nn.Softmax(dim=1)
        self.value_weights = nn.Parameter(torch.ones(self.hidden_size, self.dim_key), requires_grad=True) # (h, d)
        self.att_projection = nn.Parameter(torch.ones(self.dim_key, self.hidden_size), requires_grad=True) # (h, d)
        
        nn.init.xavier_uniform_(self.query_weigths)
        nn.init.xavier_uniform_(self.key_weigths)
        nn.init.xavier_uniform_(self.value_weights)
        nn.init.xavier_uniform_(self.att_projection)
        self.lin_1 = nn.Linear(self.dim_key, self.dim_key * 2, bias=True)
        self.relu_1 = torch.nn.ReLU(inplace=False)
        self.lin_out = nn.Linear(self.dim_key * 2, self.n_classes, bias=True)


    def forward(self, x):
        # TODO
        print("x permute:", x.shape)
        h_all, (h_last, c_last) = self.lstm_1(x) 
        print("h_last shape:", h_last.shape)
        print("h_all shape:", h_all.shape)
        print("c_last shape:", c_last.shape)
        # attention
        h_last = torch.squeeze(h_last) # (b x h)
        q = h_last @ self.query_weigths # (b x h) x (h x d) --> (b x d)
        k = h_all @ self.key_weigths # (b x s x h) x (h x d) --> (b x s x d)
        v = h_all @ self.value_weights # (b x s x h) x (h x d) --> (b x s x d)
        q_k = q @ k.permute((0, 2, 1)) / torch.sqrt(self.dim_key) # (b x d) (b x d x s) --> (b x s)
        q_k = self.att_softmax(q_k) # (b x s)
        x_att = q_k @ v # (b x s) x (b x s x d) --> (b x d)
        x_add = h_last + x_att @ self.att_projection # (b x h) + 


        print("h_last shape:", h_last.shape)
        x_1 = h_last @ self.output_weight + self.output_bias  # 1xbxh @ hxc + c = 1xbxc
        print("x_out shape:", x_1.shape)
        x_1 = x_1.squeeze(0) # bxc
        print("x_out reshaped:", x_1.shape)
        y = self.log_softmax(x_1) # bxc
        print("y shape:", y.shape)
        return y

class LstmAttClassifier(nn.Module):
    def __init__(self, n_features:int=36, win_len:int=40,
        hidden_size:int=16, att_rank:int= 6, n_classes:int=3, dropout_prob:float=0.3):
        super(LstmAttClassifier, self).__init__()

        self.n_features = n_features
        self.win_len = win_len
        self.hidden_size = hidden_size
        self.n_classes = n_classes
        self.att_rank = att_rank
        self.dropout_prob = dropout_prob

        # define parameters
        self.output_weight = nn.Parameter(torch.ones(self.hidden_size, self.n_classes), requires_grad=True) # (h, c)
        nn.init.xavier_uniform_(self.output_weight)
        self.output_bias = nn.Parameter(torch.ones(self.n_classes), requires_grad=True) # (h)
        nn.init.uniform_(self.output_bias)

        # define the layers of the model
        self.lstm_1 = nn.LSTM(self.n_features, self.hidden_size, dropout=self.dropout_prob, batch_first=True) # output (1, h)
        self.log_softmax = nn.LogSoftmax(dim=1)

        # define reduced rank multiplicative attention
        self.h_last_proj = nn.Parameter(torch.ones(self.att_rank, self.hidden_size), requires_grad=True) # (a, h)
        nn.init.xavier_uniform_(self.h_last_proj)
        self.h_all_proj = nn.Parameter(torch.ones(self.att_rank, self.hidden_size), requires_grad=True) # (a, h)
        nn.init.xavier_uniform_(self.h_all_proj)
        self.norm = nn.LayerNorm(self.hidden_size)

    def forward(self, x):
        # print("x initial:", x.shape)
        # x = torch.permute(x, (1, 0, 2))
        # print("x permute:", x.shape)
        h_all, (h_last, c_last) = self.lstm_1(x) 
        # print("h_last shape:", h_last.shape)
        # print("h_all shape:", h_all.shape)
        # print("c_last shape:", c_last.shape)
        # reduced rank mult attention
        h_all_proj = self.h_all_proj @ h_all.transpose(1, 2) # (A, H) x (B, S, H -> B, H, S) x  --> (B, A, S)
        # print("h_all_proj shape:", h_all_proj.shape)
        h_last_proj = self.h_last_proj @ h_last.permute((1, 2, 0)) # (A, H) @ (1, B, H) --> B, H, 1) --> (B, A, 1)
        # print("h_last_proj shape:", h_last_proj.shape)
        att_scores = h_all_proj.transpose(1, 2) @ h_last_proj / math.sqrt(self.att_rank) # (B, A, S -> B, S, A) x (B, A, 1) --> (B, S, 1)
        # print("att_scores:", att_scores.shape)
        att_logit = torch.softmax(att_scores.squeeze(2), dim=1) # B, S
        # print("att_logit:", att_logit.shape)
        h_att = att_logit.unsqueeze(1) @ h_all # (B, 1, S) x (B, S, H) --> (B, 1, H)
        # print("h_att:", h_att.shape)
        # add attention and last state
        h_add = self.norm(h_last.squeeze(0) + h_att.squeeze(1)) # (1, B, H -> B, H) + (B, 1, H -> B, H)
        # print("h_add:", h_add.shape)
        # output layer
        x_1 = h_add @ self.output_weight + self.output_bias  # (B, H) x (H, C) + B = (B, C)
        # print("x_out shape:", x_1.shape)
        y = self.log_softmax(x_1) # (B, C)
        # print("y shape:", y.shape)
        return y
    
class LstmMultiheadAttentionClassifier(nn.Module):
    def __init__(self, n_features:int=36, win_len:int=40,
        hidden_size:int=16, heads:int=4, att_rank:int=6, n_classes:int=3, dropout_prob:float=0.3):
        super(LstmMultiheadAttentionClassifier, self).__init__()

        self.n_features = n_features
        self.win_len = win_len
        self.hidden_size = hidden_size
        self.n_classes = n_classes
        self.att_rank = att_rank
        self.heads = heads
        self.dropout_prob = dropout_prob

        assert self.att_rank % self.heads == 0, "attention rank must be multiple of number of heads"

        # define parameters
        self.output_weight = nn.Parameter(torch.ones(self.hidden_size, self.n_classes), requires_grad=True) # (L, S)
        nn.init.xavier_uniform_(self.output_weight)
        self.output_bias = nn.Parameter(torch.ones(self.n_classes), requires_grad=True) # (L)
        nn.init.uniform_(self.output_bias)

        # define the layers of the model
        self.lstm_1 = nn.LSTM(self.n_features, self.hidden_size, dropout=self.dropout_prob, batch_first=True) # output (1, L)
        self.log_softmax = nn.LogSoftmax(dim=1)

        # define reduced rank multiplicative attention
        self.h_last_proj = nn.Parameter(torch.ones((self.heads, int(self.att_rank / self.heads), self.hidden_size)), requires_grad=True) # (H, A/H, L)
        nn.init.xavier_uniform_(self.h_last_proj)
        self.h_all_proj = nn.Parameter(torch.ones((self.heads, self.hidden_size, int(self.att_rank / self.heads))), requires_grad=True) # (H, L, A/H)
        nn.init.xavier_uniform_(self.h_all_proj)
        self.att_proj = nn.Parameter(torch.ones((self.heads * self.hidden_size, self.hidden_size)), requires_grad=True) # (H, L, A/H)
        nn.init.xavier_uniform_(self.att_proj)
        self.norm = nn.LayerNorm(self.hidden_size)

    def forward(self, x):
        '''B: Batch size, 
        H: number heads, 
        L: hidden size LSTM, 
        A: attention rank'''
        h_all, (h_last, c_last) = self.lstm_1(x) # (B, S, L) (1, B, L), (1, B, L)
        # print("h_last shape:", h_last.shape) 
        # print("h_all shape:", h_all.shape)
        # print("c_last shape:", c_last.shape)
        ## attention matrices per head
        h_att_heads_mat = self.h_all_proj @ self.h_last_proj # (H, L, A/H) x (H, A/H, L) -> (H, L, L)
        # print("h_att_heads_mat shape:", h_att_heads_mat.shape)
        ## calculate attention score per head
        h_att_all = h_all.unsqueeze(1) @ h_att_heads_mat.unsqueeze(0) # (B, 1, S, L) x (1, H, L, L) -->  (B, H, S, L)
        # print("h_att_all shape:", h_att_all.shape)
        att_scores = h_att_all @ h_last.permute((1, 2, 0)).unsqueeze(1) / math.sqrt(self.att_rank / self.heads) # (B, H, S, L) x (B, 1, L, 1) --> (B, H, S, 1)   
        # print("att_scores shape:", att_scores.shape)
        att_logits = torch.softmax(att_scores.squeeze(-1), dim=2) # (B, H, S)
        # print("att_logits shape:", att_logits.shape)
        h_att = att_logits @ h_all # (B, H, S) x (B, S, L) -> (B, H, L) 
        # print("h_att shape:", h_att.shape)
        ## concattenate head vectprs and project them to length L
        h_att_proj = h_att.reshape(h_att.size(0), -1) @ self.att_proj # (B, H*L)  x (H*L, L) -> (B, L) 
        # print("h_att_add:", h_att_add.shape)
        ## add attention to last state and normalize
        h_comb = self.norm(h_last.squeeze(0) + h_att_proj) #  (B, L -> B, L) + (B, L) -> B, L)
        # print("h_comb:", h_comb.shape)
        ## output layer
        x_1 = h_comb @ self.output_weight + self.output_bias  # (B, L) x (L, C) + B = (B, C)
        # print("x_out shape:", x_1.shape)
        y = self.log_softmax(x_1) # (B, C)
        # print("y shape:", y.shape)
        return y
