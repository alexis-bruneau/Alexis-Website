$(document).ready(function() {

    function moveToSelected(element) {

       

        // Hide the overlay for all images
        $('#carousel .image-overlay').hide();

        if (element == "next") {
            var selected = $(".selected").next();
        } else if (element == "prev") {
            var selected = $(".selected").prev();
        } else {
            var selected = element;
        }
  
        var next = $(selsected).next();
        var prev = $(selected).prev();

        $(selected).removeClass().addClass("selected");
        $(prev).removeClass().addClass("prev");
        $(next).removeClass().addClass("next");
  
        // Hide all other images
        $(next).nextAll().removeClass().addClass('hideRight');
        $(prev).prevAll().removeClass().addClass('hideLeft');

        // Show the overlay for the selected image and update its href
        var href = $(selected).data('href');  // Get the URL from the data-href attribute
        console.log(href)
        if(!/Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent)){
            $(selected).find('.image-overlay').attr('href', href).show();            
        }
        else {
            window.open(href)
        }
        
            
        
    }

    $(document).keydown(function(e) {
        switch(e.which) {
            case 37: // left
                moveToSelected('prev');
                break;

            case 39: // right
                moveToSelected('next');
                break;

            default: return;
        }
        e.preventDefault();
    });

    $('#carousel div').click(function() {
        moveToSelected($(this));
    });

    $('#prev').click(function() {
        moveToSelected('prev');
    });

    $('#next').click(function() {
        moveToSelected('next');
    });

    // Call moveToSelected on the currently selected element when the page loads
    moveToSelected($(".selected"));
});
