
from airium import from_html_to_airium

home_page_arquivo = open('paginas/home.html', 'r', encoding='utf-8')
home_page = home_page_arquivo.read()

py_str = from_html_to_airium(home_page)

print(py_str)


home_teste = open('paginas/home_teste.py', 'w', encoding='utf-8')
home_teste.write(py_str)