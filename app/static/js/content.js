$(window).on('load', function () {
    let currentPath = $(location).attr('pathname');
    let contentURL = currentPath + '/content' + $(location).attr('search');
    $.ajax({
        type: 'GET',
        url: contentURL,
        dataType: 'html',
        success: function (contentHTML) {
            $('#content').append(contentHTML);
            var dataTables = [];
            $('table.display').each(function () {
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
                    tableSettings.drawCallback = function (settings) {
                        var currentPlayer = $(this).attr('current-player');
                        if (currentPlayer)
                            $('i.fa-arrow-circle-down[player-id!="' + currentPlayer + '"').removeClass('fa-arrow-circle-down')
                                .addClass('fa-arrow-circle-right');
                    }
                } else if (thisTableId == 'todaysGames') {
                    tableSettings.paging = false;
                    tableSettings.searching = false;
                    tableSettings.info = false;
                }
                var thisDataTable = thisTable.DataTable(tableSettings);
                dataTables.push(thisDataTable);
                thisTable.find('caption').each(function () {
                    var captionText = $(this).text();
                    $(this).remove();
                    addTableTitle(thisTable, captionText);
                });
            });
            // loadDashboard();

            // Filters
            // dates with games
            $.ajax({
                type: 'GET',
                url: '/data/availableDates',
                dataType: 'json',
                async: false,
                success: function (availableDatesObj) {
                    var availableDates = availableDatesObj.data;

                    // Initialize datepickers
                    $('.datepicker').datepicker({
                        showOn: "button",
                        buttonText: '<i class="fas fa-calendar-alt datePickerIcon"></i>',
                        autoClose: true,
                        dateFormat: 'D, MM d, yy',
                        onSelect: function (date, datePicker) {
                            if (date !== datePicker.lastVal) {
                                $('#updateFiltersButton').prop('disabled', false);
                            }
                        },
                        beforeShowDay: function (date) {
                            var dateString = formatDatePickerDate(date, 'yyyy-mm-dd');
                            if ($.inArray(dateString, availableDates) != -1 || availableDates.length === 0) {
                                return [true, '', 'Available'];
                            } else {
                                return [false, '', 'unAvailable'];
                            }
                        }
                    });

                    // "Go" button on-click behavior
                    $('#updateFiltersButton').on('click', function () {
                        var filters = [];
                        ['date', 'startDate', 'endDate', 'year'].forEach(function (datePickerId) {
                            if (datePickerId === 'year') {
                                if ($('#yearPicker').length) {
                                    filters.push('year=' + $('#yearPicker').val());
                                }
                            } else {
                                var datePickerDate = $('#' + datePickerId).datepicker('getDate');
                                if (datePickerDate instanceof Date) {
                                    filters.push(datePickerId + '=' + formatDatePickerDate(datePickerDate, 'yyyy-mm-dd'));
                                }
                            }
                        });
                        $(location).attr('href', $(location).attr('pathname') + '?' + filters.join('&'));
                    });
                }
            });

            $('#spinnerDiv').addClass('hidden');
            $('#content').removeClass('hidden');
        },
        error: function () {
            $('#loadingText').text('Sorry, unable to load this page.');
            $('#spinner').addClass('hidden');
        }
    });
});