$(document).ready(function() {
  $("#menu").load("/menu_bar.html", function() {
      function initializeMenu() {
          // Show and hide main dropdown menu
          $(document).on('click', 'nav ul li a:not(:only-child)', function(e) {
              if (!$(this).siblings().hasClass('sub-menu')) {
                  $(this).siblings('.nav-dropdown').toggle();
                  $('.nav-dropdown').not($(this).siblings()).hide();
                  e.stopPropagation();
              }
          });

          // Show and hide submenu
          $(document).on('click', '.nav-dropdown > li > a', function(e) {
              e.preventDefault();
              e.stopPropagation();
              $(this).siblings('.sub-menu').toggle();
          });

          // Hide menus when clicking anywhere else on the document
          $(document).click(function() {
              $('.nav-dropdown, .sub-menu').hide();
          });

          // Toggle nav menu for small screen sizes
          $(document).on('click', '#nav-toggle', function() {
              $('nav ul').slideToggle();
              this.classList.toggle('active');
          });
      }

      initializeMenu();
  });
});
