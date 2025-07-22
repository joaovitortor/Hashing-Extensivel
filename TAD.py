from sys import argv
import io
import os

class Bucket:
    prof: int #qnt bits utilizados para endereçar as chaves
    cont: int #armazena o número de chaves
    chaves: list #lista armazena até tam_max_bucket chaves

class Diretorio:
    refs: list #lista de referencias (RRN) para buckets
    prof_dir: int #inteiro que armazena a profundidade do diretorio
    
class Hashing_extensivel:
    arq_bk: io.BufferedReader #descritor do arquivo de buckets
    dir: Diretorio # referencia para um objeto diretorio