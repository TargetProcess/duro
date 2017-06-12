var formatColumn = function(table, colNumber, formatter){
    table.find('td:nth-child(' + colNumber + ')').each(function(index, el) {
        var $node = $(el);
        if ($node.text()) {
            $node.text(formatter($node.text()))
        }
    });
};

var formatLastCreated = function (text) {
    return moment(text).fromNow()
};

var formatSeconds = function(text) {
    var time = parseInt(text);
    var formatMinutes = function(remainder) {
        if (remainder === 0) {
            return '';
        }
        var result = Math.floor(remainder / 60) + 'm ';
        if (remainder % 60 !== 0) {
            return result + remainder % 60 + 's';
        }
        else {
            return result;
        }
    };

    if (time < 60) {
        return time + 's'
    }

    if (time < 3600) {
        return formatMinutes(time);
    }
    if (time <= 86400) {
        var hours = Math.floor(time / 3600);
        var remainder = time % 3600;
        return hours + 'h ' + formatMinutes(remainder);
    }

    if (time > 86400){
        var days = Math.floor(time / 86400);
        remainder = time % 86400;
        hours = Math.floor(remainder / 3600);
        remainder = remainder % 3600;
        var result = days + 'd ';
        if (hours) {
            return result + hours + '{hours}h ' + formatMinutes(remainder);
        } else {
            return result + formatMinutes(remainder);
        }
    }
};

var formatMinutes = function (text) {
    return formatSeconds(parseInt(text) * 60);
};


var displayUpdateSuccess = function (result) {
    console.log(result);
};

var displayUpdateFailure = function (result) {
    console.log(result);
};


$(document).ready(function() {
    var $tables = $('#tables');
    $tables.DataTable({
        "paging": false,
        "info": false,
        "order": [[ 2, "desc" ]]});

    formatColumn($tables, 3, formatLastCreated);
    formatColumn($tables, 2, formatMinutes);
    formatColumn($tables, 4, formatSeconds);

    $("button").click(function (e) {
        e.preventDefault();
        var id = this.id;
        var request = {
            type: 'POST',
            url: '/update',
            data: {
                table: id.substring(12),
                tree: false
            },
            success: displayUpdateSuccess,
            error: displayUpdateFailure
        };
        if (id.substring(0, 11) === 'update-tree') {
            request.data.tree = true;
        }
        $.ajax(request);
    });
});
