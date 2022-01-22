$(document).ready(function () {
    // Font awesome icons: https://fontawesome.com/v5.0/icons?d=gallery&p=2&m=free
    var listItems = [
        ['Dashboard', 'fa-home', '/dashboard'],
        ['My Picks', 'fa-pencil-alt', '/picks'],
        ['Leaderboard', 'fa-list-ol', '/leaderboard'],
        ['Simulation', 'fa-chart-bar', '/simulations'],
        ['Games', 'fa-baseball-ball', '/games'],
        ['Players', 'fa-users', '/players'],
        ['Teams', 'fa-trophy', '/teams'],
        ['Stadiums', 'fa-university', '/stadiums'],
        ['Links', 'fa-link', '/links'],
    ];
    var fullSidebarHidden = '';
    var partialSidebarHidden = '';
    if (parseCookie('collapseSidebar') === 'false') {
        partialSidebarHidden = ' hidden';
    } else {
        fullSidebarHidden = ' hidden';
    }

    var listItemsHTML = '';
    listItems.forEach(function(item) {
        listItemsHTML += '<li title="' + item[0] + '">';
        listItemsHTML += '<a href="' + item[2] + '">';
        listItemsHTML += '<div class="fullSidebarTab' + fullSidebarHidden + '">';
        listItemsHTML += '<i class="fas ' + item[1] + '"></i>';
        listItemsHTML += '<span class="buttonText">' + item[0] + '</span>';
        listItemsHTML += '</div>';
        listItemsHTML += '<div class="container-fluid partialSidebarTab' + partialSidebarHidden + '">';
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