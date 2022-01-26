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
            // Date Picker Initialization
            $('.datepicker').datepicker({
                showOn: "button",
                buttonText: '<i class="fas fa-calendar-alt datePickerIcon"></i>',
                autoClose: true,
                dateFormat: 'D, MM d, yy',
                onSelect: function(date, datePicker) {
                    if (date !== datePicker.lastVal) {
                        var filters = [];
                        ['date', 'startDate', 'endDate', 'year'].forEach(function(datePickerId) {
                            var datePickerDate = $('#' + datePickerId).datepicker('getDate');
                            if (datePickerDate instanceof Date) {
                                filters.push(datePickerId + '=' + formatDatePickerDate(datePickerDate, 'yyyy-mm-dd'));
                            }
                        });
                        $(location).attr('href', currentPath + '?' + filters.join('&'));
                    }
                }
            })
            $('#content').removeClass('hidden');
        },
        error: function() {
            $('#loadingText').text('Sorry, unable to load this page.');
            $('#spinner').addClass('hidden');
        }
    });
});