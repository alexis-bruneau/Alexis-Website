from flask import Flask, request, jsonify, render_template,send_from_directory
import torch
from PIL import Image
import torchvision.transforms as transforms
import torchvision.models as models
import os
from app.model import CNN_HandSign 

app = Flask(__name__,
            template_folder='../templates',
            static_folder='../static')

# Define transformation
transform = transforms.Compose([transforms.Resize((224, 224)), 
                                transforms.ToTensor(), 
                                transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])])

# Load your custom model
model = CNN_HandSign()
model.load_state_dict(torch.load('app/model_weights.pth', map_location=torch.device('cpu')))
model.eval()



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
        output = model(image)
        _, predicted = torch.max(output.data, 1)
        return jsonify({'prediction': Classification_label[int(predicted)]})  # Look up the letter for the prediction
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/ASL', methods=['GET'])
def asl_page():
    return render_template('ASL.html')

@app.route('/', methods=['GET'])
def home_page():
    return render_template('index.html') 

@app.route('/<path:filename>')
def serve_static_html(filename):
    return send_from_directory('../templates', filename)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))

