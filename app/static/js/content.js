$(window).on('load', function () {
    let currentPath = $(location).attr('pathname');
    let contentURL = currentPath + '/content' + $(location).attr('search');
    $.ajax({
        type: 'GET',
        url: contentURL,
        dataType: 'html',
        success: function(contentHTML) {
            $('#content').append(contentHTML);
            $('table.display').each(function() {
                $(this).DataTable();
            });
            $('#spinnerDiv').addClass('hidden');
        },
        error: function() {
            $('#loadingText').text('Sorry, unable to load this page.');
            $('#spinner').addClass('hidden');
        }
    });
    $('#content').removeClass('hidden');
});