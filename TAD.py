from __future__ import annotations
from sys import argv
import io
import os
from struct import pack, unpack, calcsize

TAM_MAX_BUCKET: int = 5
NULO: int = -1
PED: int = 4

class Bucket:
    prof: int #qnt bits utilizados para endereçar as chaves
    cont: int #armazena o número de chaves
    chaves: list #lista armazena até tam_max_bucket chaves

    def __init__(self, prof: int, cont: int) -> None:
        self.prof = prof
        self.cont = cont
        self.chaves = [NULO for _ in range(TAM_MAX_BUCKET)]
    
    def bucket_bytes(self) -> bytes: 
        bucket_byte = pack('<I', self.prof) + pack('<I', self.cont)
        for i in range(self.cont):
            bucket_byte += pack('<I', self.chaves[i])
        for _ in range(TAM_MAX_BUCKET - self.cont):
            bucket_byte += pack('<i', NULO)
        return bucket_byte
    

class Diretorio:
    #O diretório tá ai pra: a partir do combinação relacionada a tal RRN
    #encontrarmos o bucket a ele atrelado.
    refs: list #lista de referencias (RRN) para buckets
    prof_dir: int #inteiro que armazena a profundidade do diretorio

    def __init__(self, refs: list, prof_dir: int):
        self.refs = refs
        self.prof_dir = prof_dir
    
    def diretorio_bytes(self) -> bytes:
        dir_byte = pack('<I', self.prof_dir)
        for i in range(len(self.refs)):
            dir_byte += pack('<I', self.refs[i])
        return dir_byte
        

class Hashing_extensivel:
    arq_bk: io.BufferedReader #descritor do arquivo de buckets
    dir: Diretorio # referencia para um objeto diretorio
    
    def __init__(self):
        self.arq_bk, self.dir = inicializa()

    def finaliza(self):
        with open('dir.dat') as arq_dir, open('buckets.dat') as arq_bucket:
            dir = self.dir
            arq_dir.write(dir.diretorio_bytes())
            
    def op_buscar(self, chave: int):
        endereco = gerar_endereco(chave, self.dir.prof_dir)
        ref_bk = self.dir.refs[endereco] 
        self.arq_bk.seek(PED + (ref_bk * (8 + (TAM_MAX_BUCKET * 4))))
        prof_bucket = unpack('<I', self.arq_bk.read(4))[0]
        cont_bucket = unpack('<I', self.arq_bk.read(4))[0]
        bk_encontrado = Bucket(prof_bucket, cont_bucket)
        achou = False
        for i in range(TAM_MAX_BUCKET):
            chave_bucket = unpack('<I', self.arq_bk.read(4))[0]
            if chave_bucket == chave:
                achou = True
            bk_encontrado.chaves[i] = chave_bucket
        return achou, ref_bk, bk_encontrado
        
    def op_inserir(self, chave: int):
        achou, ref_bk, bk_encontrado = self.op_buscar(chave)
        if achou:
            return False
        self.inserir_chave_bk(chave, ref_bk, bk_encontrado)
        return True
    
    def inserir_chave_bk(self, chave: int, ref_bk: int, bucket: Bucket):
        print(bucket.cont, ':c')
        if bucket.cont < TAM_MAX_BUCKET:
            print('IFFFFFF')
            bucket.chaves[bucket.cont] = chave
            self.arq_bk.seek(PED + ref_bk + 4)
            bucket.cont += 1
            self.arq_bk.write(pack('<I', bucket.cont))
            self.arq_bk.seek((bucket.cont-1) * 4, 1)
            self.arq_bk.write(pack('<I', chave))
            
        else:
            #print('ELSEEEEEEE')
            self.dividir_bk(ref_bk, bucket)
            self.op_inserir(chave)

    
    def dividir_bk(self, ref_bk: int, bucket: Bucket):
        if bucket.prof == self.dir.prof_dir:
            self.dobrar_dir()
        novo_bucket = Bucket(0, 0)

        ref_novo_bucket = 0
        for i in self.dir.refs:
            if i > ref_novo_bucket:
                ref_novo_bucket = i
        novo_inicio, novo_fim = self.encontrar_novo_intervalo(bucket)

        for i in range(novo_inicio, novo_fim):
            self.dir.refs[i] = ref_novo_bucket
        bucket.prof += 1
        novo_bucket.prof = bucket.prof

        chaves: list = []
        for chave in bucket.chaves:
            chaves.append(chave)
            self.op_remover(chave)
        for chave in chaves:
            self.op_inserir(chave)    
    
    
    def dobrar_dir(self):
        novas_refs: list = []
        for i in self.dir.refs:
            novas_refs.append(i)
            novas_refs.append(i)
        self.dir.refs = novas_refs
        self.dir.prof_dir += 1  

    def encontrar_novo_intervalo(self, bucket: Bucket):
        mascara = 1
        end_comum = gerar_endereco(bucket.chaves[0], bucket.prof)
        end_comum = end_comum << 1
        end_comum = end_comum | mascara
        bits_a_preencher = self.dir.prof_dir - (bucket.prof + 1)
        novo_inicio, novo_fim = end_comum, end_comum
        for i in range(bits_a_preencher):
            novo_inicio = novo_inicio << 1
            novo_fim = novo_fim << 1
            novo_fim = novo_fim | mascara
        return novo_inicio, novo_fim


    def op_remover(self, chave):
        achou, ref_bk, bk_encontrado = self.op_buscar(chave)
        if not achou:
            return False
        return self.remover_chave_bk(chave, ref_bk, bk_encontrado)

    def remover_chave_bk(self, chave, ref_bk: int, bucket: Bucket):
        removeu = False
        for i in range(len(bucket.chaves)):
            if bucket.chaves[i] == chave:
                chave_removida = chave
                bucket.chaves.remove(chave)
                bucket.chaves.append(NULO)
                bucket.cont -= 1
                self.arq_bk.seek(ref_bk + 8 + (bucket.cont * 4))
                self.arq_bk.write(pack('<I', chave))
                removeu = True
                break
            
        if removeu:
            self.tentar_combinar_bk(chave_removida, ref_bk, bucket)
            return True
        else:
            return False

    def tentar_combinar_bk(self, chave_removida, ref_bk: int, bucket: Bucket):
        tem_amigo, endereco_amigo = self.encontrar_bk_amigo(chave_removida, bucket)
        if tem_amigo:
            ref_amigo = self.dir.refs[endereco_amigo]
            self.arq_bk.seek(PED + (ref_amigo * (8 + (TAM_MAX_BUCKET * 4))))
            prof_bucket = unpack('<I', self.arq_bk.read(4))[0]
            cont_bucket = unpack('<I', self.arq_bk.read(4))[0]
            bk_amigo = Bucket(prof_bucket, cont_bucket)
            
            if bk_amigo.cont + bucket.cont <= TAM_MAX_BUCKET:
                bucket = self.combinar_bk(ref_bk, bucket, ref_amigo, bk_amigo)
                self.dir.refs[endereco_amigo] = ref_bk

                if self.tentar_diminuir_dir():
                    self.tentar_combinar_bk(chave_removida, ref_bk, bucket)
    

    def encontrar_bk_amigo(self, chave_removida, bucket: Bucket):
        if self.dir.prof_dir == 0:
            return False, None
        if bucket.prof < self.dir.prof_dir:
            return False, None
        end_comum =  gerar_endereco(chave_removida, bucket.prof)
        end_amigo = end_comum ^ 1
        return True, end_amigo



    def combinar_bk(self, ref_bk: int, bucket: Bucket, ref_amigo: int, bk_amigo: Bucket):
        for i in bk_amigo.chaves:
            bucket.chaves.append(i)
            bucket.cont += 1
        bucket.prof -= 1
        self.arq_bk.seek(PED + (ref_bk * (8 + (TAM_MAX_BUCKET * 4))))
        self.arq_bk.write(bucket.bucket_bytes())

        self.arq_bk.seek(PED + (ref_amigo * (8 + (TAM_MAX_BUCKET * 4))))
        bucket_nulo = Bucket(NULO, NULO)
        self.arq_bk.write(bucket_nulo.bucket_bytes())
        
        return bucket

    def tentar_diminuir_dir(self):
        if self.prof_dir == 0:
            return False
        tam_dir = 2**self.prof_dir
        diminuir = True
        for i in range(0, tam_dir-1, 2):
            if dir.refs[i] != dir.refs[i+1]:
                diminuir = False
                break
        if diminuir:
            novas_refs = []
            for i in range(0, len(self.dir.refs), 2):
                novas_refs.append(i)
            self.prof_dir -= 1
        return diminuir


def inicializa():
    if os.path.exists('dir.dat') and os.path.exists('buckets.dat'):
        with open('dir.dat', 'r+b') as arq_dir:
            arq_bucket = open("buckets.dat", 'r+b')
            prof = unpack('<I', arq_dir.read(4))[0]
            tam_dir = 2**(prof)
            refs: list[int] = []
            for _ in range(tam_dir):
                rrn = unpack('<I', arq_dir.read(4))[0]
                refs.append(rrn)
            if prof == 0:
                tam_dir -= 1
            dir = Diretorio(refs, tam_dir)
    else:
        with open('dir.dat', 'wb') as arq_dir, open('buckets.dat', 'wb') as arq_bucket:
            arq_bucket.write(pack('<I', 0))
            refs: list[int] = [0]
            bucket = Bucket(0, 0)
            arq_bucket.write(bucket.bucket_bytes())
            dir = Diretorio(refs, 0)
            arq_dir.write(dir.diretorio_bytes())
        arq_bucket = open('buckets.dat', 'r+b')
    return arq_bucket, dir


def gerar_endereco(chave: int, profundidade: int) -> int:
    '''
    Retorna uma sequência de bits extraído de uma chave (valor hash) para formar um endereço
    que possui comprimento definido pela profundidade.
    '''
    val_ret = 0
    mascara = 1

    val_hash = chave
    for _ in range(1, profundidade):
        val_ret = val_ret << 1
        bit_baixa_ordem = val_hash & mascara
        val_ret = val_ret | bit_baixa_ordem
        val_hash = val_hash >> 1
    return val_ret        

def main():

    argv = ['python TAD.py', '-e', 'op30.txt']
    if (len(argv) > 1):
        operacao = argv[1] #i insere, b busca, r remove; -pd impressao diretorio, -pb impressao bucket e só
        hashing_extensivel = Hashing_extensivel()

        if operacao == '-e' and len(argv) == 3:
            nomeArq = argv[2]
            with open(nomeArq, 'r') as arq:
                comandos = arq.readlines()
                for comando in comandos:
                    if comando[0] == 'i':
                        print(comando)
                        hashing_extensivel.op_inserir(int(comando.split()[1]))
                    elif comando[0] == 'b':
                        hashing_extensivel.op_buscar(int(comando.split()[1]))
                    elif comando[0] == 'r':
                        hashing_extensivel.op_remover(int(comando.split()[1]))
                    
        elif operacao == '-pd':
            pass
        elif operacao == '-pb':       
            pass
    else:
        print('Quantidade de comandos inválida')
        
if __name__ == '__main__':
    main()