$(document).ready(function() {
    $("#scrape-button").click(function() {
        $.ajax({
            url: "/scrape",
            type: "GET",
            success: function(response) {
                // Create a new table
                var table = $('<table></table>');

                // Iterate through the response and append rows to the table
                $.each(response, function(i, item) {
                    var row = $('<tr></tr>');

                    // Iterate through each property in the item
                    $.each(item, function(key, value) {
                        row.append($('<td></td>').text(value));
                    });

                    table.append(row);
                });

                // Append the table to the body of the page
                $('body').append(table);
            },
            error: function(error) {
                console.log(error);
            }
        });
    });
});
