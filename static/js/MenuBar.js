document.addEventListener('DOMContentLoaded', function() {
  fetch('/templates/menu_bar.html')
    .then(response => response.text())
    .then(data => {
      document.getElementById('menu').innerHTML = data;
    })
    .then(() => {
      // Function to initialize the menu
      function initializeMenu() {
        // Attach an event handler to the document object (or a closer common ancestor if possible)
        $(document).on('click', 'nav ul li a:not(:only-child)', function(e) {
          $(this).siblings('.nav-dropdown').toggle();
          // Close one dropdown when selecting another
          $('.nav-dropdown').not($(this).siblings()).hide();
          e.stopPropagation();
        });

        $(document).click(function() {
          $('.nav-dropdown').hide();
        });

        $(document).on('click', '#nav-toggle', function() {
          $('nav ul').slideToggle();
          this.classList.toggle('active');
        });
      }

      // Call the function to initialize the menu
      initializeMenu();
    });
});
