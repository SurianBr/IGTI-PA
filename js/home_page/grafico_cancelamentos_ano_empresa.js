var cancelamentos_ano_empresa = document.getElementById('cancelamentos_ano_empresa');

var tam = {
    x: |tam_x|,
    y: |tam_y|,
    type: 'scatter',
    name: 'TAM'
};

var gol = {
    x: |gol_x|,
    y: |gol_y|,
    type: 'scatter',
    name: 'GOL'
};

var azul = {
    x: |azul_x|,
    y: |azul_y|,
    type: 'scatter',
    name: 'Azul'
};


var data = [tam, gol, azul];

var layout = {
    xaxis: {
        title: 'Ano',
        autotick: false,
        dtick: 1
    },
    yaxis: {
        title: 'Cancelamentos',
        autotick: false,
        dtick: 1,
        tickformat: ",d",
        rangemode: 'tozero'
    },
    automargin: true
};

var config = {responsive: true};

Plotly.newPlot(
  cancelamentos_ano_empresa,
  data,
  layout,
  config
);