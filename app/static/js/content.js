$(window).on('load', function () {
    let currentPath = $(location).attr('pathname');
    let contentURL = currentPath + '/content' + $(location).attr('search');
    $.ajax({
        type: 'GET',
        url: contentURL,
        dataType: 'html',
        success: function(contentHTML) {
            $('#content').append(contentHTML);
            var dataTables = [];
            $('table.display').each(function() {
                var thisTable = $(this);
                var thisDataTable = thisTable.DataTable({
                    order: [],
                    rowCallback: function(row) {
                        $('td', row).each(function() {
                            $(this).html('<div class="scrollingCell">' + ($(this).text() || $(this).html()) + '</div>');
                        });
                    }
                });
                dataTables.push(thisDataTable);
            });
            let script = document.createElement('script');
            script.src = '/static/js/filters.js';
            document.head.appendChild(script);
            $('#spinnerDiv').addClass('hidden');
            $('#content').removeClass('hidden');
        },
        error: function() {
            $('#loadingText').text('Sorry, unable to load this page.');
            $('#spinner').addClass('hidden');
        }
    });
});