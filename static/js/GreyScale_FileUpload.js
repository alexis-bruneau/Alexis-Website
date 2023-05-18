$(document).ready(function() {
    // Handle file upload form submission
    $('#upload-form').submit(function(event) {
      event.preventDefault(); // Prevent the form from submitting normally
  
      // Get the file input element
      var fileInput = document.getElementById('grayscale-image');
  
      // Check if a file is selected
      if (fileInput.files.length > 0) {
        var file = fileInput.files[0];
        var formData = new FormData();
  
        // Append the file to the FormData object
        formData.append('grayscale-image', file);
  
        // Send the file data to the server using AJAX
        $.ajax({
          url: 'http://localhost:5000/upload', // Replace with your server endpoint URL
          type: 'POST',
          data: formData,
          contentType: false,
          processData: false,
          success: function(response) {
            // Handle the server response if needed
            console.log(response);
          },
          error: function(xhr, status, error) {
            // Handle the error if the upload fails
            console.error(error);
          }
        });
      }
    });
  });
  