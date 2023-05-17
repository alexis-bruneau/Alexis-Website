$(document).ready(function() {
    $("#menu").load("menu_bar.html");
  
    $("#email-link").hover(
      function() {
        $(this).attr("title", "Click on Email to copy address");
      },
      function() {
        $(this).removeAttr("title");
      }
    );
  
    $("#email-link").click(function(event) {
      event.preventDefault();
      var email = "alexisbruneauwork@gmail.com";
      navigator.clipboard.writeText(email);
    });
  });
  