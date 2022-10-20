var cancelamentos_ano_empresa_boxplot = document.getElementById('cancelamentos_ano_empresa_boxplot');

var tam = {
  y: |tam_y|,
  boxpoints: 'all',
  jitter: 0.3,
  pointpos: -1.8,
  type: 'box',
  name: 'TAM',
  customdata: |tam_x|,
  hovertemplate: '%{y:.2f}% - Ano %{customdata}',
  boxmean: true
};

var gol = {
  y: |gol_y|,
  boxpoints: 'all',
  jitter: 0.3,
  pointpos: -1.8,
  type: 'box',
  name: 'GOL',
  customdata: |gol_x|,
  hovertemplate: '%{y:.2f}% - Ano %{customdata}',
  boxmean: true
};

var azul = {
  y: |azul_y|,
  boxpoints: 'all',
  jitter: 0.3,
  pointpos: -1.8,
  type: 'box',
  name: 'AZUL',
  customdata: |azul_x|,
  hovertemplate: '%{y:.2f}% - Ano %{customdata}',
  boxmean: true
};

var data = [tam, gol, azul];

  var layout = {
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
  cancelamentos_ano_empresa_boxplot,
    data,
    layout,
    config
);