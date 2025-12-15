import streamlit as st
import psycopg2
import pandas as pd
from datetime import datetime, date
import time as t_sleep

# ==================================================
# 1. CONFIGURAÇÃO (PRIMEIRA LINHA OBRIGATÓRIA)
# ==================================================
st.set_page_config(page_title="Sistema Integrado Museu", layout="wide")

# ==================================================
# 2. CONFIGURAÇÃO DO BANCO DE DADOS
# ==================================================
DB_HOST = "tdb-ryan-ryanbacildo-adb.d.aivencloud.com"
DB_NAME = "MF_tdb"
DB_USER = "avnadmin"
DB_PASS = "AVNS_1VoGA7LlUr_MRklgkxn"
DB_PORT = "23790"

def get_connection():
    try:
        return psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS, sslmode='require'
        )
    except Exception as e:
        st.error(f"Erro de Conexão com o Banco: {e}")
        return None

# ==================================================
# 3. FUNÇÕES DE METADADOS (PARA O ADMIN)
# ==================================================

def get_tables():
    """Lista todas as tabelas públicas."""
    conn = get_connection()
    tables = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
            tables = [t[0] for t in cur.fetchall()]
        except: pass
        finally: conn.close()
    return tables

def get_pk_column(table):
    """Descobre qual é a chave primária para lógica de delete."""
    conn = get_connection()
    pk = None
    if conn:
        try:
            sql = """
                SELECT a.attname
                FROM   pg_index i
                JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE  i.indrelid = %s::regclass
                AND    i.indisprimary;
            """
            cur = conn.cursor()
            cur.execute(sql, (table,))
            res = cur.fetchone()
            if res: pk = res[0]
        except: pass
        finally: conn.close()
    return pk

def get_foreign_key_options(table):
    """Cria dropdowns para chaves estrangeiras com base nas relações do banco."""
    fk_map = {}
    conn = get_connection()
    if conn:
        try:
            sql_rels = """
                SELECT
                    kcu.column_name, 
                    ccu.table_name AS foreign_table,
                    ccu.column_name AS foreign_col
                FROM information_schema.key_column_usage AS kcu
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = kcu.constraint_name
                JOIN information_schema.table_constraints AS tc
                  ON tc.constraint_name = kcu.constraint_name
                WHERE kcu.table_name = %s AND tc.constraint_type = 'FOREIGN KEY';
            """
            cur = conn.cursor()
            cur.execute(sql_rels, (table,))
            rels = cur.fetchall()
            
            for col_name, f_tab, f_col in rels:
                # Busca nome da coluna amigável para exibição
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{f_tab}'")
                cols = [c[0] for c in cur.fetchall()]
                label = f_col 
                for l in ['nome', 'titulo', 'descricao', 'tipo']:
                    if l in cols: label = l; break
                
                # Busca opções
                cur.execute(f"SELECT DISTINCT {f_col} FROM {f_tab} ORDER BY {f_col} LIMIT 500")
                vals = [row[0] for row in cur.fetchall()]
                fk_map[col_name] = vals
        except: pass
        finally: conn.close()
    return fk_map

def apply_changes_admin(table, changes, original_df, pk_col):
    """Aplica alterações do editor (Admin) no banco."""
    conn = get_connection()
    if not conn: return False, "Sem conexão"
    
    cur = conn.cursor()
    log = []
    
    try:
        # 1. DELETE PADRÃO
        for idx in changes['deleted_rows']:
            if pk_col:
                val_pk = original_df.iloc[idx][pk_col]
                if isinstance(val_pk, (pd.Timestamp, datetime, date)): val_pk = str(val_pk)
                cur.execute(f"DELETE FROM {table} WHERE {pk_col} = %s", (val_pk,))
                log.append(f"Linha {val_pk} excluída.")
        
        # 2. INSERT (Nova Linha)
        for new_row in changes['added_rows']:
            clean_row = {k: v for k, v in new_row.items() if v is not None and v != ""}
            if clean_row:
                cols = clean_row.keys()
                vals = [clean_row[c] for c in cols]
                placeholders = ["%s"] * len(vals)
                sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(placeholders)})"
                cur.execute(sql, vals)
                log.append("Nova linha criada.")

        # 3. UPDATE (Com lógica de deletar se apagar a PK)
        for idx, edits in changes['edited_rows'].items():
            idx = int(idx)
            if pk_col:
                val_pk_original = original_df.iloc[idx][pk_col]
                if isinstance(val_pk_original, (pd.Timestamp, datetime, date)): val_pk_original = str(val_pk_original)

                # SE A PK FOI LIMPA -> DELETE
                if pk_col in edits and (edits[pk_col] is None or edits[pk_col] == ""):
                    cur.execute(f"DELETE FROM {table} WHERE {pk_col} = %s", (val_pk_original,))
                    log.append(f"Linha {val_pk_original} excluída (Chave apagada).")
                    continue

                set_parts = []
                vals = []
                for col, val in edits.items():
                    set_parts.append(f"{col} = %s")
                    vals.append(val)
                
                if set_parts:
                    vals.append(val_pk_original)
                    sql = f"UPDATE {table} SET {', '.join(set_parts)} WHERE {pk_col} = %s"
                    cur.execute(sql, vals)
                    log.append("Linha atualizada.")

        conn.commit()
        return True, "\n".join(log)
    except Exception as e:
        conn.rollback()
        return False, f"ERRO SQL: {e}"
    finally: conn.close()

# ==================================================
# 4. FUNÇÕES DE TRANSAÇÃO (PARA O VISITANTE)
# ==================================================

def fetch_museus_rotas():
    conn = get_connection()
    museus = []
    rotas = pd.DataFrame()
    if conn:
        try:
            museus = [r[0] for r in conn.cursor().execute("SELECT nome FROM museu ORDER BY nome").fetchall()]
            rotas = pd.read_sql("SELECT rota_id, tipo, valor_nota FROM rota", conn)
        except: pass
        finally: conn.close()
    return museus, rotas

def get_visitante_data(cpf):
    dados = {"nome": "", "email": "", "tel": ""}
    conn = get_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT nome, email, telefone FROM visitante WHERE cpf = %s", (cpf,))
            row = cur.fetchone()
            if row: dados = {"nome": row[0], "email": row[1], "tel": row[2]}
        except: pass
        finally: conn.close()
    return dados

def transacao_visita(cpf, nome, email, tel, museu, ids_rotas):
    conn = get_connection()
    if not conn: return False, "Sem conexão"
    
    try:
        cur = conn.cursor()
        # 1. UPSERT VISITANTE
        cur.execute("SELECT cpf FROM visitante WHERE cpf = %s", (cpf,))
        if cur.fetchone():
            cur.execute("UPDATE visitante SET nome=%s, email=%s, telefone=%s WHERE cpf=%s", (nome, email, tel, cpf))
        else:
            cur.execute("INSERT INTO visitante (cpf, nome, email, telefone) VALUES (%s, %s, %s, %s)", (cpf, nome, email, tel))
            
        # 2. INSERIR VISITA
        data_agora = datetime.now()
        cur.execute("INSERT INTO visita (museu_nome, datahora, visitante_cpf) VALUES (%s, %s, %s)", (museu, data_agora, cpf))
        
        # 3. VINCULAR ROTAS
        for rid in ids_rotas:
            cur.execute("INSERT INTO visita_percorre_rota (visita_museu_nome, visita_datahora, rota_id) VALUES (%s, %s, %s)", 
                        (museu, data_agora, rid))
            
        conn.commit()
        return True, "Visita registrada com sucesso!"
    except Exception as e:
        conn.rollback()
        return False, f"Erro na transação: {e}"
    finally: conn.close()


# ==================================================
# 5. INTERFACE PRINCIPAL
# ==================================================

modo_acesso = st.sidebar.radio("Modo de Acesso:", ["👤 Visitante (Bilheteria)", "🔧 Admin (Editor SQL)"])

# --------------------------------------------------
# MODO 1: VISITANTE
# --------------------------------------------------
if modo_acesso == "👤 Visitante (Bilheteria)":
    st.title("🏛️ Bilheteria do Museu")
    st.caption("Autoatendimento para registro de visitas.")

    cpf = st.text_input("CPF (Apenas números):", max_chars=11)
    
    dados = {"nome": "", "email": "", "tel": ""}
    if len(cpf) == 11:
        dados = get_visitante_data(cpf)
    
    if len(cpf) > 0:
        with st.form("form_visita"):
            st.markdown("### 1. Seus Dados")
            c1, c2, c3 = st.columns(3)
            nome = c1.text_input("Nome Completo", value=dados['nome'])
            email = c2.text_input("E-mail", value=dados['email'])
            tel = c3.text_input("Telefone", value=dados['tel'])
            
            st.markdown("---")
            st.markdown("### 2. Escolha o Passeio")
            
            museus_list, df_rotas = fetch_museus_rotas()
            
            if not museus_list:
                st.warning("Nenhum museu cadastrado no sistema.")
                st.form_submit_button("Indisponível", disabled=True)
            else:
                museu_sel = st.selectbox("Museu:", museus_list)
                
                # Seleção de Ingressos
                ids_selecionados = []
                if not df_rotas.empty:
                    opcoes = {f"{r['tipo']} (R$ {float(r['valor_nota']):.2f})": r['rota_id'] for i, r in df_rotas.iterrows()}
                    selecao = st.multiselect("Ingressos / Rotas:", list(opcoes.keys()))
                    ids_selecionados = [opcoes[k] for k in selecao]
                else:
                    st.info("Nenhum ingresso cadastrado.")

                submitted = st.form_submit_button("🎫 Confirmar Entrada", type="primary")
                
                if submitted:
                    if len(cpf) != 11 or not nome or not museu_sel or not ids_selecionados:
                        st.error("Preencha CPF, Nome e selecione ao menos um ingresso.")
                    else:
                        ok, msg = transacao_visita(cpf, nome, email, tel, museu_sel, ids_selecionados)
                        if ok:
                            st.balloons()
                            st.success(msg)
                            t_sleep.sleep(2)
                            st.rerun()
                        else:
                            st.error(msg)

# --------------------------------------------------
# MODO 2: ADMIN (EDITOR DE TABELAS)
# --------------------------------------------------
elif modo_acesso == "🔧 Admin (Editor SQL)":
    st.title("📝 Editor de Banco de Dados")
    
    tables = get_tables()

    if not tables:
        st.warning("Nenhuma tabela encontrada ou erro de leitura.")
        if st.button("Tentar Reconectar"): st.rerun()
    else:
        # Lógica de seleção com placeholder
        opcoes = ["(Selecione uma tabela...)"] + tables
        table = st.selectbox("Selecione a Tabela:", opcoes)
        
        if table == "(Selecione uma tabela...)":
            st.info("👆 Selecione uma tabela no menu acima para começar a editar.")
        else:
            # Carrega dados e configura editor
            conn = get_connection()
            if conn:
                try:
                    df = pd.read_sql(f"SELECT * FROM {table}", conn)
                    pk = get_pk_column(table)
                    fks = get_foreign_key_options(table)
                    
                    cfg = {}
                    for col in df.columns:
                        if col in fks:
                            cfg[col] = st.column_config.SelectboxColumn(col, options=fks[col], required=True)
                        elif 'data' in col or 'nascimento' in col:
                            cfg[col] = st.column_config.DateColumn(col, format="YYYY-MM-DD")
                        elif 'valor' in col:
                            cfg[col] = st.column_config.NumberColumn(col, format="R$ %.2f")

                    st.info(f"Tabela: **{table}** | PK: `{pk}`")
                    if table == "museu": st.caption("Dica: Obras ficam na tabela 'obra'.")

                    # O Editor (Planilha)
                    edits = st.data_editor(
                        df,
                        key="editor",
                        num_rows="dynamic",
                        use_container_width=True,
                        column_config=cfg,
                        disabled=[] # PK liberada para permitir o truque de deletar ao apagar
                    )

                    if st.button("💾 Salvar Alterações (Commit)", type="primary"):
                        change_state = st.session_state["editor"]
                        if change_state["added_rows"] or change_state["deleted_rows"] or change_state["edited_rows"]:
                            with st.spinner("Salvando..."):
                                ok, msg = apply_changes_admin(table, change_state, df, pk)
                                if ok:
                                    st.success("Sucesso!")
                                    t_sleep.sleep(1)
                                    st.rerun()
                                else:
                                    st.error(msg)
                        else:
                            st.info("Nenhuma alteração detectada.")
                
                except Exception as e:
                    st.error(f"Erro ao carregar tabela: {e}")
                finally:
                    conn.close()
