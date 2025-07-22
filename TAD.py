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

    def __init__(self, prof: int, cont: int) -> None:
        self.prof = prof
        self.cont = cont
        self.chaves = [NULO for _ in range(TAM_MAX_BUCKET)]
    
    def bucket_bytes(self) -> bytes: 
        bucket_byte = pack('<I', self.prof) + pack('<', self.cont)
        for i in range(self.cont):
            bucket_byte += pack('<I', self.chaves[i])
        for _ in range(TAM_MAX_BUCKET - self.cont):
            bucket_byte += pack('<i', NULO)
        return bucket_byte
    
    def insere_chave(self) -> None:
        pass
    


class Diretorio:
    #O diretório tá ai pra: a partir do combinação relacionada a tal RRN
    #encontrarmos o bucket a ele atrelado.
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
            arq_bucket = open("buckets.dat", 'wb')
            arq_bucket.write(pack('<I', 0))
            refs: list[int] = [0]
            bucket = Bucket(0, 0)
            arq_bucket.write(bucket.bucket_bytes())
            dir = Diretorio(refs, 0)
        
    def finaliza():
        pass
    
    def op_busca(self, chave):
        endereco = gerar_endereco(chave, self.dir.prof_dir)
        ref_bk = self.dir.refs[endereco] #nao sei se aqui seria endereco pq tem o cabecalho(talvez endereco +1?)
        
        
def gerar_endereco(self, chave: bytes, profundidade: int):
    val_ret = 0
    mascara = 1
    val_hash = unpack('<I', chave)[0]
    for _ in range(profundidade):
        val_ret = val_ret << 1
        bit_baixa_ordem = val_hash & mascara
        val_ret = val_ret | bit_baixa_ordem
        val_hash = val_hash >> 1
    return val_ret        