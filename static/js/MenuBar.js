$(document).ready(function() {
  $("#menu").load("/menu_bar.html", function() {
    function initializeMenu() {
      $(document).on('click', 'nav ul li a:not(:only-child)', function(e) {
        $(this).siblings('.nav-dropdown').toggle();
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

    initializeMenu();
  });
});
