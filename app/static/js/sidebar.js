$(document).ready(function () {let sidebarToggle = function () {
        let classMap = {
            'hidden': {
                '#fullSidebarHeader': true,
                '#partialSidebarHeader': false,
                '#sidebarCollapse': true,
                '#sidebarCollapsed': false,
                '.fullSidebarTab': true,
                '.partialSidebarTab': false
            },
            'collapsed': {
                '.components': true
            },
            'sidebarImageCollapsed': {
                '.sidebarImage': true
            }
        }

        $('#sidebar').toggleClass('collapsed');
        var collapseSidebar = $('#sidebar').hasClass('collapsed');
        adjustContentToSidebar();
        $(document).attr('cookie', 'collapseSidebar=' + collapseSidebar);

        $.each(classMap, function (className, elements) {
            $.each(elements, function (element, bool) {
                if (collapseSidebar) {
                    // Collapse sidebar menu
                    if (bool) {
                        addClass($(element), className);
                    } else {
                        removeClass($(element), className)
                    }
                } else {
                    // Expand sidebar menu
                    if (bool) {
                        removeClass($(element), className);
                    } else {
                        addClass($(element), className)
                    }
                }
            });
        });
    };

    $('#sidebarCollapse').on('click', sidebarToggle);
    $('#sidebarCollapsed').on('click', sidebarToggle);
    // let collapseSidebar = parseCookie('collapseSidebar') === 'true';
    // if (($('#sidebar').hasClass('collapsed') & !collapseSidebar) | (!$('#sidebar').hasClass('collapsed') & collapseSidebar)) {
    //     sidebarToggle();
    // }
    adjustContentToSidebar();
});
