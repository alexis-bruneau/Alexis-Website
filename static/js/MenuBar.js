document.addEventListener('DOMContentLoaded', function() {
  fetch('menu_bar.html')
    .then(response => response.text())
    .then(data => {
      document.getElementById('menu').innerHTML = data;
    })
    .then(() => {
      // Function to initialize the menu
      function initializeMenu() {
        // If a link has a dropdown, add sub menu toggle.
        $('nav ul li a:not(:only-child)').click(function(e) {
          $(this).siblings('.nav-dropdown').toggle();
          // Close one dropdown when selecting another
          $('.nav-dropdown').not($(this).siblings()).hide();
          e.stopPropagation();
        });

        // Clicking away from dropdown will remove the dropdown class
        $('html').click(function() {
          $('.nav-dropdown').hide();
        });

        // Toggle open and close nav styles on click
        $('#nav-toggle').click(function() {
          $('nav ul').slideToggle();
        });

        // Hamburger to X toggle
        $('#nav-toggle').on('click', function() {
          this.classList.toggle('active');
        });
      }

      // Call the function to initialize the menu
      initializeMenu();
    });
});
