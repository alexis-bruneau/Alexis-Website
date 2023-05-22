from flask import Flask, request, jsonify
import torch
from PIL import Image
import torchvision.transforms as transforms
import torchvision.models as models
from model import AlexNet_CNN 

app = Flask(__name__)

# Load AlexNet
alexnet = models.alexnet(pretrained=True)
alexnet.eval()

# Load your custom model
model = AlexNet_CNN(32)
model.load_state_dict(torch.load('model_weights.pth'))
model.eval()

# Define transformation
transform = transforms.Compose([transforms.Resize((224, 224)), 
                                transforms.ToTensor(), 
                                transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])])

# Set of allowed Extension for input
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg'}
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.',1)[1].lower() in ALLOWED_EXTENSIONS

# Define the classification labels
Classification_label = ['A','B','C','D','E','F','G','H','I']

@app.route('/predict', methods=['POST'])
def predict():
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file part in the request'}), 400
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected for uploading'}), 400
        if not allowed_file(file.filename):
            return jsonify({'error': 'Allowed file types are png, jpg, jpeg'}), 400
        image = Image.open(file)
        image = transform(image).unsqueeze(0)
        features = alexnet.features(image)
        output = model(features)
        _, predicted = torch.max(output.data, 1)
        return jsonify({'prediction': Classification_label[int(predicted)]})  # Look up the letter for the prediction
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
