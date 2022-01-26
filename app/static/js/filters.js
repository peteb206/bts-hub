// TO DO: store filters in cookies
// Date Picker Initialization
$('.datepicker').datepicker({
    showOn: "button",
    buttonText: '<i class="fas fa-calendar-alt datePickerIcon"></i>',
    autoClose: true,
    dateFormat: 'D, MM d, yy',
    onSelect: function(date, datePicker) {
        if (date !== datePicker.lastVal) {
            addClass($('#updateFiltersButtonInactive'), 'hidden');
            removeClass($('#updateFiltersButtonActive'), 'hidden');
        }
    }
});

$('#updateFiltersButtonActive').on('click', function() {
    var filters = [];
    ['date', 'startDate', 'endDate', 'year'].forEach(function(datePickerId) {
        if (datePickerId === 'year') {
            if ($('#yearPicker').length) {
                filters.push('year=' + $('#yearPicker').val())
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