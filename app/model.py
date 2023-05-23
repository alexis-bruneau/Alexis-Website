from torch import nn
import torch.nn.functional as F

class CNN_HandSign(nn.Module):
    def __init__(self, out_channels=10, dropout_rate = 0.25,classification_num = 9):
        super(CNN_HandSign,self).__init__()        
        self.name = 'CNN_Model'        
        self.out_channels = out_channels        
        self.conv1 = nn.Conv2d(in_channels = 3, out_channels=out_channels, kernel_size = 3, stride = 1, padding = 1)
        self.conv2 = nn.Conv2d(in_channels = out_channels, out_channels=out_channels*2, kernel_size = 3, stride = 1, padding = 1)
        self.conv3 = nn.Conv2d(in_channels = out_channels*2, out_channels=out_channels*4, kernel_size = 3, stride = 1, padding = 1)
        self.pool = nn.MaxPool2d(2,2)
        self.bn1 = nn.BatchNorm2d(out_channels)
        self.bn2 = nn.BatchNorm2d(out_channels*2)
        self.bn3 = nn.BatchNorm2d(out_channels*4)
        
        # Dropout Layer
        self.dropout = nn.Dropout(dropout_rate) # Avoid overfitting
        
        # Fully Connected Layers
        self.fc1 = nn.Linear((out_channels*4) * 28 * 28, 512)
        self.fc2 = nn.Linear(512,9)
        
       
    
    def forward(self,x):
        x = self.pool(F.relu(self.bn1(self.conv1(x)))) 
        x = self.dropout(x)
        x = self.pool(F.relu(self.bn2(self.conv2(x))))  
        x = self.dropout(x)
        x = self.pool(F.relu(self.bn3(self.conv3(x)))) 
        x = self.dropout(x)
        x = x.view(-1, (self.out_channels*4) * 28 * 28)
        x = F.relu(self.fc1(x))
        x = self.fc2(x)
        return x