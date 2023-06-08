$(document).ready(function() {
    // Submit form and display prediction message
    $("form").submit(function(e) {
      e.preventDefault();
      var formData = new FormData(this);
      $.ajax({
        url: "/predict",
        type: "POST",
        data: formData,
        success: function(response) {
          var prediction = response.prediction;
          $("#prediction-message").text("Prediction: " + prediction);
        },
        cache: false,
        contentType: false,
        processData: false
      });
    });
  
    // Image preview
    $("#image-file").change(function() {
      var input = this;
      if (input.files && input.files[0]) {
        var reader = new FileReader();
        reader.onload = function(e) {
          $("#preview-image").attr("src", e.target.result);
          $("#preview-image").show(); // Show the image
          $("#select-label").hide(); // Hide the label
        };
        reader.readAsDataURL(input.files[0]);
      } else {
        $("#preview-image").hide(); // Hide the image
        $("#select-label").show(); // Show the label
      }
    });
  });
  