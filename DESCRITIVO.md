## Objetivo:

Classe capaz de executar uma consulta/atualização/exclusão em qualquer banco de dados utilizando sqlAlchemy

## Modelos:

filter (opcional):

- matriz bidimensional contendo:
- - na primeira linha o nome da tabela
- - na segunda linha o cabeçalho de interesse
- - nas demais linhas, entre linhas (caso <> null) \_e para entre colunas e \_ou para entre linhas

header:

- matriz bidimensional contendo:
- - na primeira linha o nome da tabela
- - na segunda linha o cabeçalho de interesse

data:

- matriz bidimensional contendo:
- - na primeira linha o nome da tabela
- - na segunda linha o cabeçalho de interesse
- - nas demais linhas a informação a ser atualizada/inserida/removida
- - a última coluna sempre deve se chamada MD e só pode possuir os valores "U" -> Atualização/inclusão, "D" -> remoção
- - só pode-se fazer remoção com atualização/inclusão caso não hajá filtros e a coluna primária estiver presente

relationships (opcional):

- matriz bidimensional contendo:
- - coluna 1, "Tabela A": Nome da tabela da relação
- - coluna 2, "Tabela B": Nome da tabela da relação
- - coluna 3, "Coluna da tabela A": Nome da coluna da tabela A que se connecta com a tabela B
- - coluna 4, "Coluna da tabela B": Nome da coluna da tabela B que se conecta com a da tabela A
- - coluna 5 (opcional), "inner": 0 apenas left join, 1 (padrão) left inner join
- - regra: leitura de baixo para cima, tabela A sempre completa tabela B

\_columns_definitions:

- dicionário que armazena as definições de tabela -> coluna -> {"type":...,"primary":...,"unique":...,"default":...,"nullable":...} obtidos de metadata

\_primary_keys:

- dicionário que armazena as definições de tabela -> primary_key (null caso não exista)

## Classes principais:

DbDriver:
**init** (metadata, engine)
self.\_metadata = metadata
self.\_engine = engine
self.Pesquisar = DbDriverSearch(metadata, engine)
self.Atualizar = DbDriverUpdate(metadata, engine)

    def execute(query:string,dialect:string):
    - executa uma query passada, converte caso necessário

    def execute_stmt(stmt: statement do sqlAlchemy):
    - executa um statement passado

DbDriverCore:
**init** (metadata, engine)
self.\_metadata = metadata
self.\_engine = engine
self.\_columns_definitions = DbDriverUtils.get_columns_definitions(metadata)
self.\_primary_keys = DbDriverUtils.get_primary_keys(metadata)

    def reset():
    - Remove os atributos não privados caso existam

    def define_filter(filter: List[List[any]]):
    - Valida e salva em self.filter_positions e self.filter utilizando DbDriverUtils.get_valid_columns

    def define_relationships(relationships: List[List[any]]):
    - Valida e salva em self.relationships self.\_columns_definitions

DbDriverSearch(DbDriverCore):

    def reset(): Remove os atributos table, header e filter caso existam

    def define_header(header: List[List[any]]): - Valida e salva em self.header_positions e self.header utilizando DbDriverUtils.get_valid_columns

    def search(reset: boolean = true): Valida se é possível a pesquisa e executa, com reset = true executa self.reset()

DbDriverUpdate(DbDriverCore):

    def define_data(data: List[List[any]]): - Valida e salva em self.data_positions e self.data utilizando DbDriverUtils.get_valid_columns

    def update(reset: boolean = true):
    - Valida se é possível a atualização e executa, com reset = true executa self.reset()
    - A última coluna deve possuir "A" para atualização/inclusão e "D" para remção

## Classe de utilitários:

DbDriverUtils
def **new**(cls, \*args, \*\*kwargs):
raise TypeError("This class cannot be instantiated")

    def get_columns_definitions(metadata) -> Dict:
    - Gera um dicionário à partir de metadata na estrutura: tabela -> coluna -> {"type":...,"primary":...,"unique":...,"default":...,"nullable":...}

    def get_primary_keys(metadata) -> Dict:
    - Gera um dicionário com as chaves primárias de todas as tabelas na estrutura: tabela : coluna_chave_primarica

    def get_valid_columns(columns_definitions: Dict, matrix: list[list[any]]) -> tuple(list[int],list[list[any]]):
    - Recebe uma matriz, valida as colunas da mesma se existem em columns_definitions e retorna a posição das colunas válidas e as colunas válidas

    def is_valid_table(columns_definitions: Dict, table: str) -> boolean: valida se a tabela está em columns_definitions
    - verifica se a table está em columns_definitions

    def buid_select(columns_definitions: Dict,headers: List[List[any]],relationships: List[List[any]]=[], filters: List[List[any]]=[]) -> stmt:
    - gera o stmt sqlalchemy para o select

    def buid_update(columns_definitions: Dict, data: List[List[any]],relationships: List[List[any]] =[], filters: List[List[any]] =[]) -> stmt:
    - Gera o stmt sqlalchemy para o update

    def buid_insert(columns_definitions: Dict, data: List[List[any]]) -> stmt:
    - Gera o stmt sqlalchemy para o insert

    def buid_delete(columns_definitions: Dict, keys: List[List[any]] = [],relationships: List[List[any]] =[], filters: List[List[any]] =[] ) -> stmt:
    - Gera o stmt sqlalchemy para o delete

    def _build_filters(columns_definitions: Dict, filters: List[List[any]]) -> :
    - Gera os critérios a serem colocados dentro da cláusula where de um stmt

    def _valid_info(type: string, info: any) 0 > any:
    - Converte a info no tipo de informação válida aceita pela coluna
