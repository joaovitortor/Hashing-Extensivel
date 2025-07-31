from __future__ import annotations
from sys import argv
import io
import os
from struct import pack, unpack

TAM_MAX_BUCKET: int = 6
NULO: int = -1
PED: int = 4

class Bucket:
    prof: int
    cont: int
    chaves: list

    def __init__(self, prof: int, cont: int) -> None:
        self.prof = prof
        self.cont = cont
        self.chaves = [NULO for _ in range(TAM_MAX_BUCKET)]
    
    def bucket_bytes(self) -> bytes: 
        bucket_byte = pack('<i', self.prof) + pack('<i', self.cont)
        for i in range(self.cont):
            bucket_byte += pack('<i', self.chaves[i])
        for _ in range(TAM_MAX_BUCKET - self.cont):
            bucket_byte += pack('<i', NULO)
        return bucket_byte
    
class Diretorio:

    refs: list
    prof_dir: int 

    def __init__(self, refs: list, prof_dir: int):
        self.refs = refs
        self.prof_dir = prof_dir
    
    def diretorio_bytes(self) -> bytes:
        dir_byte = pack('<i', self.prof_dir)
        for i in range(len(self.refs)):
            dir_byte += pack('<i', self.refs[i])
        return dir_byte

class Hashing_extensivel:
    arq_bk: io.BufferedReader 
    dir: Diretorio 
    
    def __init__(self):
        self.arq_bk, self.dir = inicializa()

    def finaliza(self):

        with open('dir.dat', 'wb') as arq_dir: 
            arq_dir.truncate(0)
            arq_dir.write(pack('<i', self.dir.prof_dir))
            for i in self.dir.refs:
                arq_dir.write(pack('<i', i))
        self.arq_bk.close()

            
    def op_buscar(self, chave: int):
        endereco = gerar_endereco(chave, self.dir.prof_dir)
        ref_bk = self.dir.refs[endereco] 
        self.arq_bk.seek(PED + (ref_bk * (8 + (TAM_MAX_BUCKET * 4))))
        prof_bucket = unpack('<i', self.arq_bk.read(4))[0]
        cont_bucket = unpack('<i', self.arq_bk.read(4))[0]
        bk_encontrado = Bucket(prof_bucket, cont_bucket)
        achou = False
        for i in range(cont_bucket):
            chave_bucket = unpack('<i', self.arq_bk.read(4))[0]
            if chave_bucket == chave:
                achou = True
                print(f'Busca pela chave {chave}: chave encontrada no bucket {ref_bk}')
            bk_encontrado.chaves[i] = chave_bucket
        for i in range(cont_bucket, TAM_MAX_BUCKET):
            nulo_bucket = unpack('<i', self.arq_bk.read(4))[0]
            bk_encontrado.chaves[i] = nulo_bucket
        if not achou:
            print(f'Busca pela chave {chave}: chave não encontrada')
        return achou, ref_bk, bk_encontrado
        
    def op_inserir(self, chave: int):
        achou, ref_bk, bk_encontrado = self.op_buscar(chave)
        if achou:
            print(f'Inserção da chave {chave}: Falha - Chave duplicada.')
            return False
        self.inserir_chave_bk(chave, ref_bk, bk_encontrado)
        print(f'Inserção da chave {chave}: Sucesso.')
        return True
    
    def inserir_chave_bk(self, chave: int, ref_bk: int, bucket: Bucket):
        if bucket.cont < TAM_MAX_BUCKET:
            bucket.chaves[bucket.cont] = chave
            bucket.cont += 1
            self.arq_bk.seek(PED + (ref_bk * (8 + (TAM_MAX_BUCKET * 4))))
            self.arq_bk.write(bucket.bucket_bytes())

        else:
            self.dividir_bk(ref_bk, bucket)
            self.op_inserir(chave)

    def dividir_bk(self, ref_bk: int, bucket: Bucket):
        if bucket.prof == self.dir.prof_dir:
            self.dobrar_dir()
        prof_antiga = bucket.prof

        ref_novo_bucket = 0
        for i in self.dir.refs:
            if i > ref_novo_bucket:
                ref_novo_bucket = i
        ref_novo_bucket += 1

        bucket.prof += 1
        novo_bucket = Bucket(bucket.prof, 0) 

        chaves: list = []
        for i in range(TAM_MAX_BUCKET):
            chaves.append(bucket.chaves[i])    

        bucket.cont = 0
        bucket.chaves = [NULO for _ in range(TAM_MAX_BUCKET)]

        endereco_antigo = gerar_endereco(chaves[0], prof_antiga)

        endereco_bucket_antigo = endereco_antigo << 1 
        endereco_bucket_novo = endereco_bucket_antigo ^ 1

        for chave in chaves:
            endereco_nova_prof = gerar_endereco(chave, bucket.prof)

            if endereco_nova_prof == endereco_bucket_antigo:
                bucket.chaves[bucket.cont] = chave
                bucket.cont += 1
            elif endereco_nova_prof == endereco_bucket_novo:
                novo_bucket.chaves[novo_bucket.cont] = chave
                novo_bucket.cont += 1

        diferenca_prof = self.dir.prof_dir - bucket.prof

        inicio_bk_antigo = endereco_bucket_antigo << diferenca_prof
        fim_bk_antigo = inicio_bk_antigo | ((1 << diferenca_prof) - 1)

        inicio_bk_novo = endereco_bucket_novo << diferenca_prof
        fim_bk_novo = inicio_bk_novo | ((1 << diferenca_prof) - 1)

        for i in range(inicio_bk_antigo, fim_bk_antigo + 1):
            if i < len(self.dir.refs): 
                self.dir.refs[i] = ref_bk
                
        for i in range(inicio_bk_novo, fim_bk_novo + 1):
            if i < len(self.dir.refs): 
                self.dir.refs[i] = ref_novo_bucket

        self.arq_bk.seek(PED + (ref_bk * (8 + (TAM_MAX_BUCKET * 4))))
        self.arq_bk.write(bucket.bucket_bytes())

        self.arq_bk.seek(PED + (ref_novo_bucket * (8 + (TAM_MAX_BUCKET * 4))))
        self.arq_bk.write(novo_bucket.bucket_bytes())

    
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
            print(f'Remoção da chave {chave}: Chave não encontrada')
            return False
        else:
            print(f'Remocao da chave {chave}: Sucesso')
        return self.remover_chave_bk(chave, ref_bk, bk_encontrado)

    def remover_chave_bk(self, chave, ref_bk: int, bucket: Bucket):
        removeu = False
        indice_chave_removida = -1
        for i in range(len(bucket.chaves)):
            if bucket.chaves[i] == chave:
                indice_chave_removida = i
                chave_removida = bucket.chaves[i]
                break
        if indice_chave_removida != -1:
            bucket.chaves[indice_chave_removida] = bucket.chaves[bucket.cont -1]
            bucket.chaves[bucket.cont-1] = NULO
            bucket.cont -= 1
            self.arq_bk.seek(PED + ref_bk *(8 + (TAM_MAX_BUCKET * 4)))
            self.arq_bk.write(bucket.bucket_bytes())
            removeu = True
        
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
            prof_bucket = unpack('<i', self.arq_bk.read(4))[0]
            cont_bucket = unpack('<i', self.arq_bk.read(4))[0]
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
            if i != NULO:
                if bucket.cont < TAM_MAX_BUCKET:
                    bucket.chaves[bucket.cont] = i 
                    bucket.cont += 1

        bucket.prof -= 1
        self.arq_bk.seek(PED + (ref_bk * (8 + (TAM_MAX_BUCKET * 4))))
        self.arq_bk.write(bucket.bucket_bytes())

        self.arq_bk.seek(PED + (ref_amigo * (8 + (TAM_MAX_BUCKET * 4))))
        bucket_nulo = Bucket(NULO, 0)
        bucket_nulo.chaves = [NULO for _ in range(TAM_MAX_BUCKET)]
        self.arq_bk.write(bucket_nulo.bucket_bytes())
        
        return bucket

    def tentar_diminuir_dir(self):
        if self.dir.prof_dir == 0:
            return False
        tam_dir = 2**self.dir.prof_dir
        diminuir = True
        for i in range(0, tam_dir-1, 2):
            if self.dir.refs[i] != self.dir.refs[i+1]:
                diminuir = False
                break
        if diminuir:
            novas_refs = []
            for i in range(0, len(self.dir.refs), 2):
                novas_refs.append(self.dir.refs[i])
            self.dir.prof_dir -= 1
        return diminuir
    
    
    def print_buckets(self):
        TAM_BUCKET_BYTES = (4 + 4 + (TAM_MAX_BUCKET * 4))
        self.arq_bk.seek(PED, io.SEEK_SET) 

        num_buckets = (os.path.getsize('buckets.dat') - PED)// TAM_BUCKET_BYTES
        print(f"----- Buckets -----")
        for rrn in range(num_buckets):
            self.arq_bk.seek(PED + (rrn * TAM_BUCKET_BYTES))
            prof_bucket = unpack('<i', self.arq_bk.read(4))[0]
            cont_bucket = unpack('<i', self.arq_bk.read(4))[0]
            
            bucket_lido = Bucket(prof_bucket, cont_bucket)
            
            for i in range(TAM_MAX_BUCKET):
                chave_lida = unpack('<i', self.arq_bk.read(4))[0]
                bucket_lido.chaves[i] = chave_lida
            if prof_bucket == -1:
                print(f"Bucket {rrn} -- Removido --\n")
            else:
                print(f"Bucket {rrn} (Prof = {bucket_lido.prof}):")
                print(f"Conta_chaves = {bucket_lido.cont}")
                print(f"Chaves = {bucket_lido.chaves[:bucket_lido.cont]}\n") 


    def print_diretorio(self):
        print("\n----- Diretório -----")
        print(f"Profundidade = {self.dir.prof_dir}")
        print(f"Tamanho atual = {len(self.dir.refs)}")

        rrns_unicos = sorted(list(set(self.dir.refs)))

        print(f'Total de buckets = {len(rrns_unicos)}')

        for i, rrn in enumerate(self.dir.refs):
            print(f"dir[{i}] = bucket({rrn})")
        print("\n")

def inicializa():
    if os.path.exists('dir.dat') and os.path.exists('buckets.dat'):
        with open('dir.dat', 'rb') as arq_dir:
            arq_bucket = open("buckets.dat", 'r+b')
            prof = unpack('<i', arq_dir.read(4))[0]
            tam_dir = 2**(prof)
            refs: list[int] = []
            for _ in range(tam_dir):
                rrn = unpack('<i', arq_dir.read(4))[0]
                refs.append(rrn)
            dir = Diretorio(refs, prof)
    else:
        with open('dir.dat', 'w+b') as arq_dir, open('buckets.dat', 'w+b') as arq_bucket:
            arq_bucket.write(pack('<i', 0))
            refs: list[int] = [0]
            bucket = Bucket(0, 0)
            arq_bucket.write(bucket.bucket_bytes())
            dir = Diretorio(refs, 0)
            arq_dir.write(dir.diretorio_bytes())
        arq_bucket = open('buckets.dat', 'r+b')
    return arq_bucket, dir



def gerar_endereco(chave: int, profundidade: int) -> int:
    val_ret = 0
    mascara = 1
    val_hash = chave
    for _ in range(profundidade):
        val_ret = val_ret << 1
        bit_baixa_ordem = val_hash & mascara
        val_ret = val_ret | bit_baixa_ordem
        val_hash = val_hash >> 1
    return val_ret


def main():
    if (len(argv) > 1):
        operacao = argv[1] 
        hashing_extensivel = Hashing_extensivel()

        if operacao == '-e' and len(argv) == 3:
            nomeArq = argv[2]
            with open(nomeArq, 'r') as arq:
                comandos = arq.readlines()
                for comando in comandos:
                    if comando[0] == 'i':
                        hashing_extensivel.op_inserir(int(comando.split()[1]))
                    elif comando[0] == 'b':
                        hashing_extensivel.op_buscar(int(comando.split()[1]))
                    elif comando[0] == 'r':
                        hashing_extensivel.op_remover(int(comando.split()[1]))

        elif operacao == '-pd':
            hashing_extensivel.print_diretorio()
        elif operacao == '-pb':
            hashing_extensivel.print_buckets()

        hashing_extensivel.finaliza()
    else:
        print('Quantidade de comandos inválida')
        
if __name__ == '__main__':
    main()