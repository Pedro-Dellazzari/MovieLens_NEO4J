import csv
from py2neo import Graph, Node
import os
import time

# Aguardar o Neo4j no Docker
time.sleep(15)

N_FILMES = 5000
N_AVALIACOES = 5000
N_TAGS = 5000
N_LINKS = 5000

# NEO4J_HOST será fornecido pelo Docker, caso contrário, localhost

HOST = os.environ.get("NEO4J_HOST", "localhost")
PORTA = 7687
USUARIO = "neo4j"
SENHA = "neo4j"  # padrão

grafo = Graph("bolt://" + HOST + ":7687", auth=(USUARIO, SENHA))

def main():

    criarNosDeGenero()

    print("Passo 1 de 4: carregando nós de filmes")
    carregarFilmes()

    print("Passo 2 de 4: carregando relacionamentos de avaliação")
    carregarAvaliacoes()

    print("Passo 3 de 4: carregando relacionamentos de tags")
    carregarTags()

    print("Passo 4 de 4: atualizando links para nós de filmes")
    carregarLinks()

def criarNosDeGenero():
    todosGeneros = ["Ação", "Aventura", "Animação", "Infantil", "Comédia", "Crime",
                    "Documentário", "Drama", "Fantasia", "Film-Noir", "Terror", "Musical",
                    "Mistério", "Romance", "Ficção Científica", "Suspense", "Guerra", "Faroeste"]

    for genero in todosGeneros:
        gen = Node("Gênero", nome=genero)
        grafo.create(gen)


def carregarFilmes():
    with open('data/filmes.csv') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        next(readCSV, None)  # pular cabeçalho
        for i, linha in enumerate(readCSV):

            criarNosDeFilme(linha)
            criarRelacionamentosDeGeneroFilme(linha)

            if (i % 100 == 0):
                print(f"{i}/{N_FILMES} Nós de filme criados")

            # interromper após N_FILMES filmes

            if i >= N_FILMES:
                break

def criarNosDeFilme(linha):
    dadosFilme = analisarLinhaFilme(linha)
    id = dadosFilme[0]
    titulo = dadosFilme[1]
    ano = dadosFilme[2]
    filme = Node("Filme", id=id, titulo=titulo, ano=ano)
    grafo.create(filme)

def analisarLinhaFilme(linha):
        id = linha[0]
        ano = linha[1][-5:-1]
        titulo = linha[1][:-7]

        return (id, titulo, ano)


def criarRelacionamentosDeGeneroFilme(linha):
    idFilme = linha[0]
    generosFilme = linha[2].split("|")

    for generoFilme in generosFilme:
        grafo.run('MATCH (g:Gênero {nome: {genero}}), (m:Filme {id: {idFilme}}) CREATE (g)-[:É_DO_GÊNERO_DE]->(m)',
            genero=generoFilme, idFilme=idFilme)

def analisarLinhaRelacionamentosGeneroFilme(linha):
    idFilme = linha[0]
    generosFilme = linha[2].split("|")

    return (idFilme, generosFilme)

def carregarAvaliacoes():
    with open('data/avaliacoes.csv') as csvfile:
         readCSV = csv.reader(csvfile, delimiter=',')
         next(readCSV, None) #pular cabeçalho
         for i,linha in enumerate(readCSV):
             criarNosDeUsuario(linha)
             criarRelacionamentoDeAvaliacao(linha)

             if (i % 100 == 0):
                 print(f"{i}/{N_AVALIACOES} Relacionamentos de avaliação criados")

             if (i >= N_AVALIACOES):
                 break

def criarNosDeUsuario(linha):
    usuario = Node("Usuário", id="Usuário " + linha[0])
    grafo.merge(usuario, "Usuário", "id")

def criarRelacionamentoDeAvaliacao(linha):
    dadosAvaliacao = analisarLinhaRelacionamentosAvaliacao(linha)

    grafo.run(
        'MATCH (u:Usuário {id: {idUsuario}}), (m:Filme {id: {idFilme}}) CREATE (u)-[:CLASSIFICOU { avaliacao: {avaliacao}, timestamp: {timestamp} }]->(m)',
        idUsuario=dadosAvaliacao[0], idFilme=dadosAvaliacao[1], avaliacao=dadosAvaliacao[2], timestamp=dadosAvaliacao[3])

def analisarLinhaRelacionamentosAvaliacao(linha):
    idUsuario = "Usuário " + linha[0]
    idFilme = linha[1]
    avaliacao = float(linha[2])
    timestamp = linha[3]

    return (idUsuario, idFilme, avaliacao, timestamp)

def carregarTags():
    with open('data/tags.csv') as csvfile:
         readCSV = csv.reader(csvfile, delimiter=',')
         next(readCSV, None) #pular cabeçalho
         for i,linha in enumerate(readCSV):
             criarRelacionamentoDeTag(linha)

             if (i % 100 == 0):
                 print(f"{i}/{N_TAGS} Relacionamentos de tag criados")

             if (i >= N_TAGS):
                 break

def criarRelacionamentoDeTag(linha):
    dadosTag = analisarLinhaRelacionamentosTag(linha)

    grafo.run(
        'MATCH (u:Usuário {id: {idUsuario}}), (m:Filme {id: {idFilme}}) CREATE (u)-[:MARCOU { tag: {tag}, timestamp: {timestamp} }]->(m)',
        idUsuario=dadosTag[0], idFilme=dadosTag[1], tag=dadosTag[2], timestamp=dadosTag[3])

def analisarLinhaRelacionamentosTag(linha):
    idUsuario = "Usuário " + linha[0]
    idFilme = linha[1]
    tag = linha[2]
    timestamp = linha[3]

    return (idUsuario, idFilme, tag, timestamp)

def carregarLinks():
    with open('data/links.csv') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        next(readCSV, None)  # pular cabeçalho
        for i, linha in enumerate(readCSV):

            atualizarNoDeFilmeComLinks(linha)

            if (i % 100 == 0):
                print(f"{i}/{N_LINKS} Nós de filme atualizados com links")

            # interromper após N_LINKS filmes

            if i >= N_LINKS:
                break

def atualizarNoDeFilmeComLinks(linha):
    dadosLink = analisarLinhaLinks(linha)

    grafo.run(
        'MATCH (m:Filme {id: {idFilme}}) SET m += { imdbId: {imdbId} , tmdbId: {tmdbId} }',
        idFilme=dadosLink[0], imdbId=dadosLink[1], tmdbId=dadosLink[2])

def analisarLinhaLinks(linha):
    idFilme = linha[0]
    imdbId = linha[1]
    tmdbId = linha[2]

    return (idFilme, imdbId, tmdbId)


if __name__ == '__main__':
    main()
