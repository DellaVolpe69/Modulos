import streamlit as st
import pandas as pd
import supabase
import sys
import subprocess
from pathlib import Path, PureWindowsPath
from datetime import datetime, timedelta
import os
import tempfile
import base64
import io
from PIL import Image, ImageOps

# Configuração da página
st.set_page_config(page_title="Metas TDV", layout="wide")

# Configuração Supabase
modulos_dir = Path(__file__).parent / "Modulos"

# Se o diretório ainda não existir, faz o clone direto do GitHub
if not modulos_dir.exists():
    print("📥 Clonando repositório Modulos do GitHub...")
    subprocess.run(
        [
            "git",
            "clone",
            "https://github.com/DellaVolpe69/Modulos.git",
            str(modulos_dir),
        ],
        check=True,
    )

# Garante que o diretório está no caminho de importação
if str(modulos_dir) not in sys.path:
    sys.path.insert(0, str(modulos_dir))
import Modulos.Minio.examples.MinIO as meu_minio

from Modulos import AzureLogin
from Modulos import ConectionSupaBase

# Inicializa conexão Supabase
supabase = ConectionSupaBase.conexao()

# ================================================
# AUTENTICAÇÃO AZURE — CAPTURA DO USUÁRIO LOGADO
# ================================================
# O módulo AzureLogin NÃO expõe uma função: ele executa o fluxo OAuth
# ao ser importado e, no final, grava o resultado em st.session_state
# ("user_email" e "user_name"). A simples importação acima (from Modulos
# import AzureLogin) já dispara esse fluxo. Aqui apenas lemos o resultado.
user_email = (st.session_state.get("user_email") or "").lower()

if not user_email or user_email == "desconhecido":
    st.error("Não foi possível identificar o usuário autenticado.")
    st.stop()

# ================================================
# CONTROLE DE ACESSO
# ================================================
# Apenas este usuário pode EDITAR compromissos da semana.
# Os demais podem criar e visualizar normalmente.
EDITOR_AUTORIZADO = "anderson.junior@dellavolpe.com"
pode_editar = user_email == EDITOR_AUTORIZADO.lower()

# Constantes de Tabelas
TABELA_EQUIPES = "EQUIPES_4DX"
TABELA_USUARIOS = "USUARIOS_4DX"
TABELA_METAS = "METAS_CRUCIAIS_4DX"
TABELA_MEDIDAS = "MEDIDAS_DIRECAO_4DX"
TABELA_SEMANAS = "SEMANAS_4DX"
TABELA_METAS_4DX = "METAS_4DX"
TABELA_ATRIBUICAO_METAS_4DX = "ATRIBUICAO_METAS_4DX"


# Funções utilitárias Supabase
def run_query(table_name):
    """Retorna um DataFrame com os dados da tabela."""
    try:
        response = supabase.table(table_name).select("*").execute()
        data = response.data
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        st.error(f"Erro ao carregar dados de {table_name}: {e}")
        return pd.DataFrame()


def insert_data(table_name, data):
    """Insere dados na tabela."""
    try:
        response = supabase.table(table_name).insert(data).execute()
        return response.data
    except Exception as e:
        st.error(f"Erro ao inserir em {table_name}: {e}")
        return None


def update_data(table_name, data, match_col, match_val):
    """Atualiza dados na tabela."""
    try:
        response = (
            supabase.table(table_name).update(data).eq(match_col, match_val).execute()
        )
        return response.data
    except Exception as e:
        st.error(f"Erro ao atualizar em {table_name}: {e}")
        return None


def delete_data(table_name, match_col, match_val):
    """Deleta dados da tabela."""
    try:
        response = (
            supabase.table(table_name).delete().eq(match_col, match_val).execute()
        )
        return response.data
    except Exception as e:
        st.error(f"Erro ao deletar de {table_name}: {e}")
        return None


def criar_equipe(nome_equipe):
    """Cria uma nova equipe no Supabase."""
    try:
        df_equipes = run_query(TABELA_EQUIPES)
        if not df_equipes.empty and "equipe" in df_equipes.columns:
            if nome_equipe in df_equipes["equipe"].values:
                st.warning("Essa equipe já existe.")
                return False
        insert_data(TABELA_EQUIPES, {"equipe": nome_equipe})
        st.success("Equipe cadastrada com sucesso!")
        return True
    except Exception as e:
        st.error(f"Erro ao criar equipe: {e}")
        return False


def criar_usuario(nome, email, equipe, foto_base64=None):
    """Cria um novo usuário no Supabase."""
    try:
        df_usuarios = run_query(TABELA_USUARIOS)
        if not df_usuarios.empty and "email" in df_usuarios.columns:
            if email in df_usuarios["email"].values:
                st.warning("Esse email já está cadastrado.")
                return False
        user_data = {"nome": nome, "email": email, "equipe": equipe}
        if foto_base64:
            user_data["foto_url"] = foto_base64
        insert_data(TABELA_USUARIOS, user_data)
        st.success("Usuário cadastrado com sucesso!")
        return True
    except Exception as e:
        st.error(f"Erro ao criar usuário: {e}")
        return False


def criar_meta_crucial(equipe, responsavel, meta_crucial, indicador, meta_final, prazo):
    """Cria ou atualiza uma meta crucial no Supabase."""
    try:
        delete_data(TABELA_METAS, "responsavel", responsavel)
        insert_data(
            TABELA_METAS,
            {
                "equipe": equipe,
                "responsavel": responsavel,
                "meta_crucial": meta_crucial,
                "indicador": indicador,
                "meta_final": meta_final,
                "prazo": prazo,
            },
        )
        st.success("Meta salva com sucesso!")
        return True
    except Exception as e:
        st.error(f"Erro ao salvar meta: {e}")
        return False


def criar_meta(tipo, descricao, periodo, unidade, valor):
    """Cria uma nova meta na tabela METAS_4DX."""
    try:
        valor_formatado = f"{valor}%" if unidade == "%" else valor
        response = (
            supabase.table(TABELA_METAS_4DX)
            .insert(
                {
                    "tipo": tipo,
                    "descricao": descricao,
                    "periodo": periodo,
                    "unidade": unidade,
                    "valor": valor_formatado,
                }
            )
            .execute()
        )
        if response.data:
            st.success("Meta cadastrada com sucesso!")
            return True
        else:
            st.error("Erro ao cadastrar meta: Nenhum dado retornado.")
            return False
    except Exception as e:
        st.error(f"Erro ao cadastrar meta: {e}")
        return False


def criar_medida_direcao(responsavel, meta_crucial, medidas, frequencia):
    """Cria novas medidas de direção no Supabase."""
    try:
        for medida in medidas:
            if medida.strip():
                response = (
                    supabase.table(TABELA_MEDIDAS)
                    .select("*")
                    .eq("responsavel", responsavel)
                    .eq("meta_crucial", meta_crucial)
                    .eq("medida_direcao", medida.strip())
                    .execute()
                )
                if response.data:
                    st.warning(
                        f"A medida '{medida.strip()}' já está cadastrada para o responsável '{responsavel}' e meta '{meta_crucial}'."
                    )
                    return False
                insert_data(
                    TABELA_MEDIDAS,
                    {
                        "responsavel": responsavel,
                        "meta_crucial": meta_crucial,
                        "medida_direcao": medida.strip(),
                        "frequencia": frequencia,
                    },
                )
        st.success("Medidas salvas com sucesso!")
        return True
    except Exception as e:
        st.error(f"Erro ao salvar medidas: {e}")
        return False


def atualizar_medida_direcao(
    row_id, medida, frequencia, responsavel, meta_crucial, medida_antiga=None
):
    """Atualiza uma medida de direção no Supabase."""
    try:
        if row_id:
            update_data(
                TABELA_MEDIDAS,
                {"medida_direcao": medida, "frequencia": frequencia},
                "id",
                row_id,
            )
        else:
            supabase.table(TABELA_MEDIDAS).update(
                {"medida_direcao": medida, "frequencia": frequencia}
            ).eq("responsavel", responsavel).eq("meta_crucial", meta_crucial).eq(
                "medida_direcao", medida_antiga
            ).execute()
        st.success("Medida atualizada com sucesso!")
        return True
    except Exception as e:
        st.error(f"Erro ao atualizar medida: {e}")
        return False


def registrar_semana(
    responsavel,
    meta_crucial,
    semana_ref,
    compromisso,
    concluido,
    evidencia,
    porcentagem,
    semana_proxima=False,
):
    """
    Registra um compromisso no Supabase, evitando duplicidade.
    Se concluido for "Sim", registra a data atual como data_conclusao.
    """
    try:
        response = (
            supabase.table(TABELA_SEMANAS)
            .select("*")
            .eq("responsavel", responsavel)
            .eq("meta_crucial", meta_crucial)
            .eq("semana_ref", semana_ref)
            .eq("compromisso", compromisso)
            .execute()
        )
        if response.data:
            st.warning(
                f"Esse compromisso já está cadastrado para o responsável '{responsavel}' na semana '{semana_ref}'."
            )
            return False
        data = {
            "responsavel": responsavel,
            "meta_crucial": meta_crucial,
            "semana_ref": semana_ref,
            "compromisso": compromisso,
            "concluido": concluido,
            "evidencia": evidencia,
            "porcentagem": porcentagem,
            "semana_proxima": semana_proxima,
        }
        if concluido == "Sim":
            data["data_conclusao"] = datetime.now().date().isoformat()
        insert_data(TABELA_SEMANAS, data)
        return True
    except Exception as e:
        st.error(f"Erro ao registrar compromisso: {e}")
        return False


def atribuir_meta(meta_id, responsavel, equipe):
    """Atribui uma meta a um responsável e equipe, evitando duplicidade."""
    try:
        response = (
            supabase.table(TABELA_ATRIBUICAO_METAS_4DX)
            .select("*")
            .eq("meta_id", meta_id)
            .eq("responsavel", responsavel)
            .execute()
        )
        if response.data:
            st.warning(f"Essa meta já está atribuída ao responsável '{responsavel}'.")
            return False
        insert_data(
            TABELA_ATRIBUICAO_METAS_4DX,
            {"meta_id": meta_id, "responsavel": responsavel, "equipe": equipe},
        )
        st.success("Meta atribuída com sucesso!")
        return True
    except Exception as e:
        st.error(f"Erro ao atribuir meta: {e}")
        return False


def listar_metas():
    """Lista todas as metas cadastradas na tabela METAS_4DX."""
    try:
        response = supabase.table(TABELA_METAS_4DX).select("*").execute()
        if response.data:
            df_metas = pd.DataFrame(response.data)
            st.dataframe(df_metas, use_container_width=True)
        else:
            st.info("Nenhuma meta cadastrada.")
    except Exception as e:
        st.error(f"Erro ao listar metas: {e}")


def listar_metas_atribuidas(responsavel=None, equipe=None):
    """Lista metas atribuídas a um responsável ou equipe."""
    try:
        df_atribuicoes = run_query(TABELA_ATRIBUICAO_METAS_4DX)
        if responsavel:
            df_atribuicoes = df_atribuicoes[
                df_atribuicoes["responsavel"] == responsavel
            ]
        if equipe:
            df_atribuicoes = df_atribuicoes[df_atribuicoes["equipe"] == equipe]
        metas_ids = df_atribuicoes["meta_id"].tolist()
        df_metas = run_query(TABELA_METAS_4DX)
        df_metas = df_metas[df_metas["id"].isin(metas_ids)]
        df_result = pd.merge(df_atribuicoes, df_metas, left_on="meta_id", right_on="id")
        return df_result
    except Exception as e:
        st.error(f"Erro ao listar metas atribuídas: {e}")
        return pd.DataFrame()


def upload_foto_minio(foto):
    """Redimensiona a foto para 200x266 pixels e converte para uma string Base64."""
    if foto is not None:
        try:
            img = Image.open(foto)
            img = ImageOps.fit(img, (200, 266), Image.LANCZOS)
            output = io.BytesIO()
            img.save(output, format="WEBP", quality=80)
            output.seek(0)
            imagem_bytes = output.read()
            imagem_base64 = base64.b64encode(imagem_bytes).decode("utf-8")
            return imagem_base64
        except Exception as e:
            st.error(f"Erro ao processar a foto: {e}")
            return None
    return None


def inicio_semana(d=None):
    d = d or datetime.today()
    return (d - timedelta(days=d.weekday())).date()


def semana_anterior():
    return inicio_semana() - timedelta(days=7)


def atualizar_foto_usuario(email, foto_base64):
    """Atualiza a foto de um usuário existente no Supabase."""
    try:
        data = {"foto_url": foto_base64}
        update_data(TABELA_USUARIOS, data, "email", email)
        st.success("Foto do usuário atualizada com sucesso!")
        return True
    except Exception as e:
        st.error(f"Erro ao atualizar foto: {e}")
        return False


# UI
st.title("Metas - TDV 🎯")

tabs = st.tabs(
    [
        "👥 Equipes & Usuários",
        "📝 Cadastro M.C / M.D",
        "🔗 Atribuição de Metas",
        "📋 Compromissos",
        "📊 Visão Geral",
    ]
)

# TAB 0 – EQUIPES & USUÁRIOS
with tabs[0]:
    st.subheader("Cadastro de Equipes")
    with st.form("form_equipe"):
        equipe = st.text_input("Equipe", key="nova_equipe")
        if st.form_submit_button("Salvar") and equipe:
            criar_equipe(equipe)
            st.rerun()

    st.divider()
    st.subheader("Cadastro de Usuários")
    if st.session_state.get("usuario_ok", False):
        st.success("Usuário cadastrado com sucesso!")
        st.session_state.usuario_ok = False

    df_eq = run_query(TABELA_EQUIPES)
    if df_eq.empty:
        st.info("Cadastre uma equipe primeiro.")
    else:
        with st.form("form_user"):
            nome = st.text_input("Nome", key="user_nome")
            email = st.text_input("Email", key="user_email")
            equipe = st.selectbox("Equipe", df_eq["equipe"], key="user_equipe")
            foto = st.file_uploader(
                "Foto do Usuário", type=["jpg", "jpeg", "png"], key="user_foto"
            )

            if st.form_submit_button("Salvar"):
                foto_base64 = upload_foto_minio(foto)
                if foto and foto_base64 is None:
                    st.error("Erro ao processar a foto.")
                    st.stop()

                if criar_usuario(nome, email, equipe, foto_base64):
                    st.session_state.usuario_ok = True
                    st.rerun()

    st.divider()
    st.subheader("Editar Foto do Usuário")

    df_usuarios = run_query(TABELA_USUARIOS)
    if df_usuarios.empty:
        st.info("Nenhum usuário cadastrado.")
    else:
        with st.form("form_edit_user"):
            email = st.selectbox(
                "Selecione o Usuário (por email)",
                options=df_usuarios["email"].tolist(),
                key="edit_user_email",
            )
            nova_foto = st.file_uploader(
                "Nova Foto do Usuário",
                type=["jpg", "jpeg", "png"],
                key="edit_user_foto",
            )

            if st.form_submit_button("Atualizar Foto"):
                foto_base64 = upload_foto_minio(nova_foto)
                if nova_foto and foto_base64 is None:
                    st.error("Erro ao processar a foto.")
                    st.stop()

                if atualizar_foto_usuario(email, foto_base64):
                    st.rerun()

# TAB 1 – META CRUCIAL
with tabs[1]:
    st.subheader("Cadastro de Meta Crucial / Medida de Direção")
    with st.form("form_nova_meta"):
        tipo_meta = st.selectbox(
            "Tipo de Meta", ["Meta Crucial", "Medida de Direção"], key="tipo_meta"
        )
        descricao = st.text_input("Descritivo", max_chars=100, key="descricao_meta")
        periodo = st.selectbox(
            "Período",
            [
                "Janeiro",
                "Fevereiro",
                "Março",
                "Abril",
                "Maio",
                "Junho",
                "Julho",
                "Agosto",
                "Setembro",
                "Outubro",
                "Novembro",
                "Dezembro",
            ],
            key="periodo_meta",
        )
        unidade = st.selectbox("Unidade", ["%", "Número inteiro"], key="unidade_meta")
        valor = st.text_input("Valor", key="valor_meta")

        if st.form_submit_button("Salvar"):
            if not descricao:
                st.error("O descritivo é obrigatório.")
            else:
                if criar_meta(tipo_meta, descricao, periodo, unidade, valor):
                    st.rerun()

    st.divider()
    st.subheader("Metas Cadastradas")
    listar_metas()

# TAB 2 – ATRIBUIÇÃO DE METAS
with tabs[2]:
    st.subheader("Atribuir Metas a Responsáveis")
    df_metas = run_query(TABELA_METAS_4DX)
    df_equipes = run_query(TABELA_EQUIPES)
    df_usuarios = run_query(TABELA_USUARIOS)

    if df_metas.empty or df_equipes.empty or df_usuarios.empty:
        st.warning("Cadastre metas, equipes e usuários antes de atribuir.")
    else:
        metas_dict = {
            row["descricao"]: row["id"]
            for _, row in df_metas[["id", "descricao"]].iterrows()
        }
        if not metas_dict:
            st.warning("Nenhuma meta cadastrada para atribuir.")
        else:
            descricao_selecionada = st.selectbox(
                "Meta",
                options=list(metas_dict.keys()),
                key="meta_atribuicao",
            )
            meta_id = metas_dict[descricao_selecionada]
            equipe = st.selectbox(
                "Equipe", options=df_equipes["equipe"].tolist(), key="equipe_atribuicao"
            )
            usuarios_equipe = df_usuarios[df_usuarios["equipe"] == equipe]
            responsavel = st.selectbox(
                "Responsável",
                options=usuarios_equipe["nome"].tolist(),
                key="responsavel_atribuicao",
            )
            if st.button("Atribuir Meta"):
                if atribuir_meta(meta_id, responsavel, equipe):
                    st.rerun()

# TAB 3 – COMPROMISSOS
with tabs[3]:
    st.subheader("📋 Compromissos de Medidas de Direção")
    df_atribuidas = listar_metas_atribuidas()
    if df_atribuidas.empty:
        st.info("Nenhuma meta atribuída encontrada.")
    else:
        df_atribuidas = df_atribuidas[df_atribuidas["tipo"] == "Medida de Direção"]
        if df_atribuidas.empty:
            st.info("Nenhuma medida de direção atribuída encontrada.")
        else:
            for equipe, grupo in df_atribuidas.groupby("equipe"):
                st.markdown(f"## 🏷️ {equipe}")
                for idx, m in grupo.iterrows():
                    with st.expander(f"🎯 {m['descricao']} — {m['responsavel']}"):
                        st.write(f"**Descrição:** {m['descricao']}")
                        semana_atual = str(inicio_semana())
                        response_semana_atual = (
                            supabase.table(TABELA_SEMANAS)
                            .select("*")
                            .eq("responsavel", m["responsavel"])
                            .eq("meta_crucial", m["descricao"])
                            .eq("semana_ref", semana_atual)
                            .execute()
                        )
                        if not response_semana_atual.data:
                            st.subheader("Compromisso da Semana Atual")
                            compromisso_atual = st.text_input(
                                f"Compromisso",
                                key=f"compromisso_atual_{idx}",
                            )

                            if st.button(
                                f"Salvar Compromisso Atual",
                                key=f"salvar_compromisso_atual_{idx}",
                            ):
                                if registrar_semana(
                                    m["responsavel"],
                                    m["descricao"],
                                    semana_atual,
                                    compromisso_atual,
                                    "Não",
                                    None,
                                    0,
                                ):
                                    st.success(
                                        "✅ Compromisso da semana atual salvo com sucesso!"
                                    )
                                    st.rerun()
                        else:
                            st.info("✅ Compromisso da semana atual já foi preenchido.")

                        semana_proxima = str(
                            inicio_semana(datetime.now() + timedelta(days=7))
                        )
                        st.subheader("Compromisso para a Próxima Semana")
                        compromisso_prox = st.text_input(
                            f"Compromisso para a próxima semana",
                            key=f"compromisso_prox_{idx}",
                        )

                        if st.button(
                            f"Salvar Compromisso (Próxima Semana)",
                            key=f"salvar_compromisso_prox_{idx}",
                        ):
                            if registrar_semana(
                                m["responsavel"],
                                m["descricao"],
                                semana_proxima,
                                compromisso_prox,
                                "Não",
                                None,
                                0,
                                True,
                            ):
                                st.success(
                                    "✅ Compromisso para a próxima semana salvo com sucesso!"
                                )
                                st.rerun()

# TAB 4 – VISÃO GERAL
with tabs[4]:
    st.subheader("📊 Visão Geral dos Compromissos")

    # Aviso de permissão para quem não pode editar
    if not pode_editar:
        st.info(
            "🔒 Você está em modo de visualização. "
            "Apenas o usuário autorizado pode alterar os compromissos da semana."
        )

    df_equipes = run_query(TABELA_EQUIPES)
    df_usuarios = run_query(TABELA_USUARIOS)

    if df_equipes.empty or df_usuarios.empty:
        st.warning("Cadastre equipes e usuários antes de visualizar.")
    else:
        equipe_selecionada = st.selectbox(
            "Selecione a Equipe",
            options=df_equipes["equipe"].tolist(),
            key="equipe_visao_geral",
        )
        usuarios_equipe = df_usuarios[df_usuarios["equipe"] == equipe_selecionada]
        responsavel_selecionado = st.selectbox(
            "Selecione o Responsável",
            options=usuarios_equipe["nome"].tolist(),
            key="responsavel_visao_geral",
        )
        response_compromissos = (
            supabase.table(TABELA_SEMANAS)
            .select("*")
            .eq("responsavel", responsavel_selecionado)
            .order("semana_ref", desc=False)
            .execute()
        )

        if not response_compromissos.data:
            st.info("Nenhum compromisso encontrado para este responsável.")
        else:
            df_compromissos = pd.DataFrame(response_compromissos.data)
            st.subheader(f"Compromissos de {responsavel_selecionado}")

            for idx, row in df_compromissos.iterrows():
                row_id = row.get("id", idx)
                with st.expander(
                    f"Editar: {row['meta_crucial']} — {row['semana_ref']}"
                ):
                    compromisso = st.text_input(
                        "Compromisso:",
                        value=row["compromisso"],
                        key=f"compromisso_edit_{row_id}_{responsavel_selecionado}",
                        disabled=not pode_editar,
                    )

                    concluido = st.selectbox(
                        "Concluído?",
                        options=["Sim", "Não"],
                        index=0 if row["concluido"] == "Sim" else 1,
                        key=f"concluido_edit_{row_id}_{responsavel_selecionado}",
                        disabled=not pode_editar,
                    )

                    porcentagem_valor = (
                        int(row["porcentagem"]) if pd.notna(row.get("porcentagem")) else 0
                    )
                    porcentagem = st.number_input(
                        "Porcentagem de Conclusão (%)",
                        min_value=0,
                        max_value=100,
                        value=porcentagem_valor,
                        key=f"porcentagem_edit_{row_id}_{responsavel_selecionado}",
                        disabled=not pode_editar,
                    )

                    evidencia = None
                    if concluido == "Sim":
                        evidencia = st.file_uploader(
                            "Anexar Evidência",
                            type=["jpg", "jpeg", "png"],
                            key=f"evidencia_edit_{row_id}_{responsavel_selecionado}",
                            disabled=not pode_editar,
                        )
                    else:
                        st.warning("⚠️ Para anexar evidência, marque como 'Concluído'.")

                    # Botão de salvar visível apenas para o usuário autorizado
                    if pode_editar:
                        if st.button(
                            "Salvar Alterações",
                            key=f"salvar_edit_{row_id}_{responsavel_selecionado}",
                        ):
                            evidencia_base64 = (
                                upload_foto_minio(evidencia)
                                if evidencia
                                else row["evidencia"]
                            )

                            update_data_dict = {
                                "compromisso": compromisso,
                                "concluido": concluido,
                                "porcentagem": porcentagem,
                                "evidencia": evidencia_base64,
                            }

                            if concluido == "Sim" and row["concluido"] != "Sim":
                                update_data_dict["data_conclusao"] = (
                                    datetime.now().date().isoformat()
                                )

                            update_data(
                                TABELA_SEMANAS,
                                update_data_dict,
                                "id",
                                row["id"],
                            )
                            st.success("✅ Alterações salvas com sucesso!")
                            st.rerun()
                    else:
                        st.caption("🔒 Edição restrita ao usuário autorizado.")
