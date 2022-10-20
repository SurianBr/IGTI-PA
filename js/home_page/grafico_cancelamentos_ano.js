var voos_ano = document.getElementById('cancelamentos_ano');

var total_voos = {
    x: |x_cancelamentos|,
    y: |y_cancelamentos|,
    type: 'scatter'
};

var data = [
    total_voos
];

var layout = {
    xaxis: {
        title: 'Ano',
        autotick: false,
        dtick: 1
    },
    yaxis: {
        title: '% Cancelamentos',
        autotick: false,
        dtick: 1,
        tickformat: ",d",
        rangemode: 'tozero'
    },
    automargin: true
};

var config = {responsive: true}

Plotly.newPlot(
    voos_ano,
    data,
    layout,
    config
);