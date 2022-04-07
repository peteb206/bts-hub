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
                var tableSettings = {
                    paging: true,
                    lengthChange: true,
                    searching: true,
                    info: true,
                    order: [],
                    rowCallback: dataTablesRowCallback
                }
                var thisTableId = thisTable.attr('id');
                if (thisTableId == 'eligibleBatters') {
                    tableSettings.lengthChange = false;
                    tableSettings.info = false;
                } else if (thisTableId == 'todaysGame') {
                    tableSettings.paging = false;
                    tableSettings.searching = false;
                    tableSettings.info = false;
                }
                var thisDataTable = thisTable.DataTable(tableSettings);
                dataTables.push(thisDataTable);
                thisTable.find('caption').each(function() {
                    var captionText = $(this).text();
                    $(this).remove();
                    addTableTitle(thisTable, captionText);
                });
            });
            // loadDashboard();
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