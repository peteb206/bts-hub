$(document).ready(function () {
    var listItems = [
        ['Dashboard', 'fa-home', '/dashboard'],
        ['My Picks', 'fa-pencil-alt', '/picks'],
        ['Games', 'fa-baseball-ball', '/games'],
        ['Players', 'fa-users', '/players'],
        ['Teams', 'fa-trophy', '/teams'],
        ['Stadiums', 'fa-university', '/stadiums'],
        ['Links', 'fa-link', '/links'],
    ];
    var listItemsHTML = '';
    listItems.forEach(function(item) {
        listItemsHTML += '<li title="' + item[0] + '">';
        listItemsHTML += '<a href="' + item[2] + '">';
        listItemsHTML += '<div class="fullSidebarTab">';
        listItemsHTML += '<i class="fas ' + item[1] + '"></i>';
        listItemsHTML += '<span class="buttonText">' + item[0] + '</span>';
        listItemsHTML += '</div>';
        listItemsHTML += '<div class="container-fluid partialSidebarTab hidden">';
        listItemsHTML += '<i class="fas ' + item[1] + ' fa-lg"></i>';
        listItemsHTML += '</div>';
        listItemsHTML += '</a>';
        listItemsHTML += '</li>';
    });
    $('#sidebarTabs').append(listItemsHTML);

    var currentEndpoint = $(location).attr('pathname');
    $('a').each(function() {
        var linkEndpoint = $(this).attr('href');
        if (linkEndpoint == currentEndpoint) {
            $(this).attr('href', '#');
            $(this).parent().addClass('active');
        } else {
            $(this).parent().removeClass('active');
        }
    });
});