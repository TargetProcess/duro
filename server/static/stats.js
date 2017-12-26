(function ($) {
    const updateStats = stats => {
        let text = `Last 24h: 
        ${stats.tables} tables, ${stats.updates} updates. 
        Load: ${stats.load}%.`;
        $('#stats').html(text)
    };

    $(document).ready(function () {
        $.ajax({url: '/api/stats', success: updateStats});
    });
})(jQuery);
