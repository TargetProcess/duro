let chart = new tauCharts.Chart({
    data: datasource,
    type: 'line',
    x: 'date',
    y: 'duration',
    dimensions: {
        date: {
        type: 'measure',
        scale: 'time'
        },
        duration: { type: 'measure'}
    },
    guide: {
        x: {label: 'Creation start'},
        y: {label: 'Creation duration'},
        interpolate: 'smooth-keep-extremum',
        showAnchors: 'always'

    },
    plugins: [
        tauCharts.api.plugins.get('tooltip')(),
        tauCharts.api.plugins.get('legend')()

    ]
});
chart.renderTo('#chart');