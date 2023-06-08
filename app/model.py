from torch import nn
import torch.nn.functional as F

class CNN_HangSign_New(nn.Module):
    def __init__(self, out_channels=5, dropout_rate = 0.25,classification_num = 10):
        super(CNN_HangSign_New,self).__init__()        
        self.name = 'CNN_Model'        
        self.out_channels = out_channels
        
        self.conv1 = nn.Conv2d(in_channels = 3, out_channels=out_channels, kernel_size = 3, stride = 1, padding = 1)
        self.conv2 = nn.Conv2d(in_channels = out_channels, out_channels=out_channels*2, kernel_size = 3, stride = 1, padding = 1)
        self.conv3 = nn.Conv2d(in_channels = out_channels*2, out_channels=out_channels*4, kernel_size = 3, stride = 1, padding = 1)
        self.conv4 = nn.Conv2d(in_channels = out_channels*4, out_channels=out_channels*8, kernel_size = 3, stride = 1, padding = 1)

        self.pool = nn.MaxPool2d(2,2)
        
        self.bn1 = nn.BatchNorm2d(out_channels)
        self.bn2 = nn.BatchNorm2d(out_channels*2)
        self.bn3 = nn.BatchNorm2d(out_channels*4)
        self.bn4 = nn.BatchNorm2d(out_channels*8)
        
        self.dropout = nn.Dropout(dropout_rate)
        
        self.fc1 = nn.Linear((out_channels*8) * 14 * 14, 1024)
        self.fc2 = nn.Linear(1024, 512)
        self.fc3 = nn.Linear(512,classification_num)
        
    def forward(self,x):
        x = self.pool(F.relu(self.bn1(self.conv1(x)))) 
        x = self.dropout(x)
        x = self.pool(F.relu(self.bn2(self.conv2(x))))  
        x = self.dropout(x)
        x = self.pool(F.relu(self.bn3(self.conv3(x)))) 
        x = self.dropout(x)
        x = self.pool(F.relu(self.bn4(self.conv4(x)))) 
        x = self.dropout(x)
        x = x.view(-1, (self.out_channels*8) * 14 * 14)
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.fc3(x)
        return x