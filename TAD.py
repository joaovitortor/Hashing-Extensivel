from sys import argv
import io
import os
from struct import pack, unpack, calcsize

TAM_MAX_BUCKET: 5
NULO = -1
PED = 4

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
        for i in range(self.refs):
            dir_byte += pack('<I', self.refs[i])
        return dir_byte
        
    
class Hashing_extensivel:
    arq_bk: io.BufferedReader #descritor do arquivo de buckets
    dir: Diretorio # referencia para um objeto diretorio
    
    def __init__(self, arq_bk: io.BufferedReader, dir: Diretorio):
        self.arq_bk = arq_bk
        self.dir = dir

    def inicializa(self):
        if os.path.exists("dir.dat") and os.path.exists("buckets.dat"):
            with open("buckets.dat", 'br') as arq_bucket, open("dir.dat", 'br') as arq_dir:
                prof = unpack('<I', arq_dir.read(4))[0]
                tam_dir = 2**(prof)
                refs: list[int] = []
                for _ in range(tam_dir):
                    rrn = unpack('<I', arq_dir.read(4))[0]
                    refs.append(rrn)
                dir = Diretorio(refs, tam_dir)
        else:
            with open("dir.dat", "wb") as arq_dir, open("buckets.dat", "wb") as arq_bucket:
                arq_bucket.write(pack('<I', 0))
                refs: list[int] = [0]
                bucket = Bucket(0, 0)
                arq_bucket.write(bucket.bucket_bytes())
                dir = Diretorio(refs, 0)
                arq_dir.write(dir.diretorio_bytes())
        self.diretorio = dir
        
    def finaliza(self):
        with open("dir.dat") as arq_dir, open("buckets.dat") as arq_bucket:
            dir = self.dir
            arq_dir.write(dir.diretorio_bytes())

    def op_buscar(self, chave):
        endereco = gerar_endereco(chave, self.dir.prof_dir) #00001001 -> 4
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
        
    def op_inserir(self, chave):
        achou, ref_bk, bk_encontrado = self.op_buscar(chave)
        if achou:
            return False
        self.inserir_chave_bk(chave, ref_bk, bk_encontrado)
        return True
    
    def inserir_chave_bk(self, chave, ref_bk: int, bucket: Bucket):
        if bucket.cont < TAM_MAX_BUCKET:
            bucket.chaves[bucket.cont] = chave
            self.arq_bk.seek(ref_bk + 8 + (bucket.cont * 4))
            self.arq_bk.write(pack('<I', chave))
        else:
            self.dividir_bk(ref_bk, bucket)
            self.op_inserir(chave)

    
    def dividir_bk(self, ref_bk: int, bucket: Bucket):
        if bucket.prof == self.prof_dir:
            self.dobrar_dir()
        prof_ant = bucket.prof
        bucket.prof += 1
        novo_bucket = Bucket(bucket.prof, 0)
        
        maior = 0
        for i in range(2**self.prof_dir):
            if i > maior:
                maior = i
        ref_novo_bucket = maior

        novo_inicio, novo_fim = self.encontrar_novo_intervalo(bucket)
        aux = 0
        for i in range(ref_bk, 2**self.dir.prof_dir):
            if i == ref_bk:
                aux += 1
        #terminarrrrrrrrrrrrrr

        novo_bucket.prof = bucket.prof

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
        bits_a_preencher = self.dir_prof - (bucket.prof + 1)
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
        bucket.chaves.remove(chave)
        chave_removida = chave
        bucket.cont -= 1
        #reescreva bucket em ref_bk no arquivo de buckets
        removeu = True
        if removeu:
            self.tentar_combinar_bk(chave_removida, ref_bk, bucket)
        else:
            return False
    #Tem que ver isso aquiiiiiiiiiii


    def tentar_combinar_bk(self, chave_removida, ref_bk: int, bucket: Bucket):
        tem_amigo, endereco_amigo = self.dividir_bkencontrar_bk_amigo(chave_removida, bucket)
        if not tem_amigo:
            return
        bk_amigo = endereco_amigo 
        if (bk_amigo.cont + bucket.cont) <= TAM_MAX_BUCKET:
            ref_amigo = dir.refs[endereco_amigo]
            bucket = self.combinar_bk(ref_bk, bucket, ref_amigo, bk_amigo)
            #continuaaaaaaa
    
    def encontrar_bk_amigo(self, chave_removida, bucket: Bucket):
        if self.prof_dir == 0:
            return False, None
        if bucket.prof < self.prof_dir:
            return False, None
        #Encontre o end_comum das chaves contidas em bucket
        #Encontre o endereço do bucket amigo (end_amigo)
        #Continuaaaaaa

    def combinar_bk(self, ref_bk: int, bucket: Bucket, ref_amigo, bk_amigo: Bucket):
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


def gerar_endereco(chave, profundidade: int) -> int:
    '''
    Retorna uma sequência de bits extraído de uma chave (valor hash) para formar um endereço
    que possui comprimento definido pela profundidade.
    '''
    val_ret = 0
    mascara = 1
    chave_bytes = pack('<I', chave)
    val_hash = unpack('<I', chave_bytes)[0]
    for _ in range(profundidade):
        val_ret = val_ret << 1
        bit_baixa_ordem = val_hash & mascara
        val_ret = val_ret | bit_baixa_ordem
        val_hash = val_hash >> 1
    return val_ret        

def main():
    print(gerar_endereco(10011000, 3))



if __name__ == '__main__':
    main()