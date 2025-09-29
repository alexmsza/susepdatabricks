import os
import csv

# Define o caminho para o diretório com os arquivos CSV
dir_path = 'C:\Users\usuario\Desktop\susep\Autoseg2021A'

# Lista todos os arquivos no diretório
try:
    all_files = os.listdir(dir_path)
    # Filtra para manter apenas os arquivos CSV
    csv_files = [f for f in all_files if f.endswith('.csv')]
except FileNotFoundError:
    print(f"Erro: O diretório não foi encontrado em '{dir_path}'")
    csv_files = []

# Itera sobre cada arquivo CSV encontrado
for filename in csv_files:
    file_path = os.path.join(dir_path, filename)
    print(f"\n--- Analisando: {filename} ---")

    try:
        # Abre o arquivo usando a codificação 'latin-1', comum em arquivos brasileiros.
        # O parâmetro newline='' é importante para o módulo csv.
        with open(file_path, mode='r', encoding='latin-1', newline='') as infile:
            # Cria um leitor de CSV com o delimitador de ponto e vírgula
            reader = csv.reader(infile, delimiter=';')

            # Lê e imprime o cabeçalho
            header = next(reader)
            print(f"Cabeçalho: {header}")

            # Lê e imprime as 10 primeiras linhas de dados
            print("10 primeiras linhas:")
            for i in range(10):
                try:
                    row = next(reader)
                    print(row)
                except StopIteration:
                    # Caso o arquivo tenha menos de 10 linhas
                    print("Fim do arquivo alcançado antes de completar 10 linhas.")
                    break
    except Exception as e:
        # Captura outros erros, como problemas de permissão ou arquivos corrompidos
        print(f"Ocorreu um erro ao processar o arquivo {filename}: {e}")

print("\nAnálise concluída.")
