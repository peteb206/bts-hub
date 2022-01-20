$(document).ready(function () {
    let addClass = function(element, className) {
        if (!element.hasClass(className)) {
            element.addClass(className);
        }
    }
    let removeClass = function(element, className) {
        if (element.hasClass(className)) {
            element.removeClass(className);
        }
    }
    let sidebarToggle = function () {
        let classMap = {
            'hidden': {
                '#sidebarCollapseText': true,
                '#fullSidebarHeader': true,
                '#partialSidebarHeader': false,
                '#sidebarCollapse': true,
                '#sidebarCollapsed': false,
                '.fullSidebarTab': true,
                '.partialSidebarTab': false,
                '.buttonText': true
            },
            'shrunk': {
                'img': true
            }
        }

        $('#sidebar').toggleClass('collapsed');
        var shrinkMenu = $('#sidebar').hasClass('collapsed');

        $.each(classMap, function (className, elements) {
            $.each(elements, function (element, bool) {
                if (shrinkMenu) {
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
    // $(window).resize(sidebarToggle(false)).trigger('resize');
});