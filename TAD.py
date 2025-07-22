from sys import argv
import io
import os
from struct import pack, unpack, calcsize

TAM_MAX_BUCKET: 5
NULO = -1

class Bucket:
    prof: int #qnt bits utilizados para endereçar as chaves
    cont: int #armazena o número de chaves
    chaves: list #lista armazena até tam_max_bucket chaves


class Diretorio:
    refs: list #lista de referencias (RRN) para buckets
    prof_dir: int #inteiro que armazena a profundidade do diretorio

    def __init__(self, refs: list, prof_dir: int):
        self.refs = refs
        self.prof_dir = prof_dir
        
    
class Hashing_extensivel:
    arq_bk: io.BufferedReader #descritor do arquivo de buckets
    dir: Diretorio # referencia para um objeto diretorio
    
    def __init__(self, arq_bk: io.BufferedReader, dir: Diretorio):
        self.arq_bk = arq_bk
        self.dir = dir

    def inicializa(self):
        if os.path.exists("dir.dat") and os.path.exists("buckets.dat"):
            arq_bucket = open("buckets.dat", 'br')
            arq_dir = open("dir.dat", 'br')
            prof = unpack('<I', arq_dir.read(4))[0]
            tam_dir = 2**(prof)
            refs: list[int] = []
            for _ in range(tam_dir):
                rrn = unpack('<I', arq_dir.read(4))[0]
                refs.append(rrn)
            diretorio = Diretorio(refs, tam_dir)
        else:
            arq_bucket = open("buckets.dat", 'b')
            refs: list[int] = []
            dir = Diretorio(refs, 0)

        
    def finaliza():
        pass


