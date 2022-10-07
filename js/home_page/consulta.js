var editor = ace.edit("editor");
editor.setTheme("ace/theme/monokai");
editor.session.setMode("ace/mode/mysql");

document.getElementById("form_query").onsubmit = function(evt) {
    var valor = editor.getValue();

    // Transforma fim de linha em | para envio via get
    valor = valor.replace(/\n/g, "|");
    document.getElementById("editortext").value = valor;
}