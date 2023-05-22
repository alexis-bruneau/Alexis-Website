from torch import nn
import torch.nn.functional as F

class AlexNet_CNN(nn.Module):
    def __init__(self, out_channel=32):
        super(AlexNet_CNN, self).__init__()
        self.out_channel = out_channel
        self.name = f"AlexNet_CNN"
        self.conv1 = nn.Conv2d(256, out_channel, 3) # in_channels, out_chanels, kernel_size
        self.pool = nn.MaxPool2d(2, 2) # kernel_size, stride -> polling reduces size by 2 
        self.fc1 = nn.Linear(out_channel*2*2, 128)
        self.fc2 = nn.Linear(128, 9)

    def forward(self, x):
        x = self.pool(F.relu(self.conv1(x)))        
        x = x.view(-1, self.out_channel*2*2)
        x = F.relu(self.fc1(x))
        x = self.fc2(x)
        return x
