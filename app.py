import streamlit as st
import psycopg2
from psycopg2 import extras
import bcrypt
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from decimal import Decimal, ROUND_HALF_UP, getcontext

# Precisión alta para cálculos financieros (evita errores por float binario)
getcontext().prec = 28

# =========================================================
# 1. GESTIÓN DE CONEXIÓN
# =========================================================
def get_connection():
    """Crea o recupera la conexión, asegurándose de que esté activa."""
    if "db_conn" not in st.session_state or st.session_state.db_conn.closed != 0:
        try:
            st.session_state.db_conn = psycopg2.connect(
                host=st.secrets["postgres"]["host"],
                database=st.secrets["postgres"]["database"],
                user=st.secrets["postgres"]["user"],
                password=st.secrets["postgres"]["password"],
                port=st.secrets["postgres"]["port"]
            )
        except Exception as e:
            st.error(f"Error crítico de conexión: {e}")
            return None
    return st.session_state.db_conn

def get_cursor():
    """Obtiene un cursor fresco de una conexión activa."""
    conn = get_connection()
    if conn:
        return conn, conn.cursor(cursor_factory=extras.DictCursor)
    return None, None

# =========================================================
# 2. MOTOR FINANCIERO — MERCADO PARAGUAYO (PYG)
# =========================================================
# Reglas aplicadas:
#   • Moneda: Guaraní (Gs.) — sin centavos, redondeo HALF_UP a entero.
#   • Tasas: se interpretan como NOMINAL MENSUAL (costumbre P2P en PY).
#   • Sistema por defecto: Francés (cuota fija) — estándar BCP.
#   • IVA 10% sobre intereses (Ley 125/91) — informativo para P2P informal.
#   • CET: Costo Efectivo Total anualizado capitalizando mensualmente.
# =========================================================

GUARANI = Decimal("1")              # Unidad mínima: 1 Gs.
IVA_PY = Decimal("0.10")            # 10% IVA sobre intereses
MESES_ANIO = Decimal("12")

def gs(valor) -> Decimal:
    """Redondea cualquier valor a guaraní entero (HALF_UP)."""
    if not isinstance(valor, Decimal):
        valor = Decimal(str(valor))
    return valor.quantize(GUARANI, rounding=ROUND_HALF_UP)

def fmt_gs(valor) -> str:
    """Formatea Gs. con separador de miles al estilo paraguayo."""
    return f"{int(gs(valor)):,}".replace(",", ".") + " Gs."

def calcular_prestamo(capital, tasa_mensual_pct, plazo_meses, sistema="Francés"):
    """
    Calcula cuotas de un préstamo con precisión Decimal.

    Args:
        capital: Monto principal en Gs.
        tasa_mensual_pct: Tasa nominal mensual en % (ej: 5 → 5% mensual).
        plazo_meses: Cantidad de cuotas mensuales.
        sistema: 'Francés' (cuota fija), 'Alemán' (amortización fija) o 'Simple'.

    Returns:
        dict con: cuota, total_pagar, total_intereses, iva_intereses,
                  tea (tasa efectiva anual), cet_anual, cronograma (list).
    """
    C = Decimal(str(capital))
    i = Decimal(str(tasa_mensual_pct)) / Decimal("100")   # tasa mensual decimal
    n = int(plazo_meses)

    if C <= 0 or n <= 0:
        return None

    cronograma = []
    saldo = C

    # ---------- Sistema Francés (cuota fija) ----------
    if sistema == "Francés":
        if i == 0:
            cuota = C / Decimal(n)
        else:
            # cuota = C · [i(1+i)^n] / [(1+i)^n − 1]
            factor = (Decimal("1") + i) ** n
            cuota = C * (i * factor) / (factor - Decimal("1"))

        cuota_r = gs(cuota)
        for k in range(1, n + 1):
            interes_k = gs(saldo * i)
            # Última cuota ajusta residuo para cerrar el saldo en 0.
            if k == n:
                capital_k = saldo
                cuota_k = gs(capital_k + interes_k)
            else:
                capital_k = gs(cuota_r - interes_k)
                cuota_k = cuota_r
            saldo = gs(saldo - capital_k)
            cronograma.append({
                "Cuota Nº": k,
                "Capital": int(capital_k),
                "Interés": int(interes_k),
                "IVA s/Int.": int(gs(interes_k * IVA_PY)),
                "Cuota": int(cuota_k),
                "Saldo": int(max(saldo, Decimal("0"))),
            })

    # ---------- Sistema Alemán (amortización constante) ----------
    elif sistema == "Alemán":
        amort = C / Decimal(n)
        for k in range(1, n + 1):
            interes_k = gs(saldo * i)
            capital_k = gs(amort) if k < n else saldo
            cuota_k = gs(capital_k + interes_k)
            saldo = gs(saldo - capital_k)
            cronograma.append({
                "Cuota Nº": k,
                "Capital": int(capital_k),
                "Interés": int(interes_k),
                "IVA s/Int.": int(gs(interes_k * IVA_PY)),
                "Cuota": int(cuota_k),
                "Saldo": int(max(saldo, Decimal("0"))),
            })

    # ---------- Interés Simple (no compuesto) ----------
    else:  # "Simple"
        interes_total = gs(C * i * Decimal(n))
        cuota_fija = gs((C + interes_total) / Decimal(n))
        for k in range(1, n + 1):
            capital_k = gs(C / Decimal(n)) if k < n else saldo
            interes_k = gs(interes_total / Decimal(n))
            cuota_k = gs(capital_k + interes_k)
            saldo = gs(saldo - capital_k)
            cronograma.append({
                "Cuota Nº": k,
                "Capital": int(capital_k),
                "Interés": int(interes_k),
                "IVA s/Int.": int(gs(interes_k * IVA_PY)),
                "Cuota": int(cuota_k),
                "Saldo": int(max(saldo, Decimal("0"))),
            })

    total_pagar = sum(Decimal(r["Cuota"]) for r in cronograma)
    total_intereses = sum(Decimal(r["Interés"]) for r in cronograma)
    iva_intereses = gs(total_intereses * IVA_PY)

    # Tasa Efectiva Anual (capitalización mensual compuesta)
    tea = ((Decimal("1") + i) ** 12 - Decimal("1")) * Decimal("100")
    # CET ≈ TEA en P2P sin comisiones/seguros. Si se agregaran, va aquí.
    cet_anual = tea

    return {
        "cuota_promedio": gs(total_pagar / Decimal(n)),
        "total_pagar": gs(total_pagar),
        "total_intereses": gs(total_intereses),
        "iva_intereses": iva_intereses,
        "tasa_mensual": Decimal(str(tasa_mensual_pct)),
        "tea": tea.quantize(Decimal("0.01")),
        "cet_anual": cet_anual.quantize(Decimal("0.01")),
        "cronograma": cronograma,
    }

# =========================================================
# 3. INICIALIZACIÓN DE TABLAS (idempotente)
# =========================================================
def init_db():
    conn, cur = get_cursor()
    if not cur:
        return
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE,
                password TEXT,
                role TEXT DEFAULT 'user'
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS movimientos (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES usuarios(id),
                tipo TEXT,
                monto DECIMAL,
                descripcion TEXT,
                fecha DATE
            )
        """)
        # Añadir categoría a movimientos (migración segura)
        cur.execute("ALTER TABLE movimientos ADD COLUMN IF NOT EXISTS categoria TEXT DEFAULT 'Otros'")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS prestamos (
                id SERIAL PRIMARY KEY,
                prestamista_id INTEGER REFERENCES usuarios(id),
                prestatario_id INTEGER REFERENCES usuarios(id),
                monto DECIMAL,
                estado TEXT DEFAULT 'pendiente',
                interes DECIMAL DEFAULT 5.0,
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Columnas nuevas para cálculos financieros (ALTER seguro)
        for col, ddl in [
            ("plazo_meses",       "ALTER TABLE prestamos ADD COLUMN IF NOT EXISTS plazo_meses INTEGER DEFAULT 12"),
            ("sistema",           "ALTER TABLE prestamos ADD COLUMN IF NOT EXISTS sistema TEXT DEFAULT 'Francés'"),
            ("fecha_vencimiento", "ALTER TABLE prestamos ADD COLUMN IF NOT EXISTS fecha_vencimiento DATE"),
            ("cuota_mensual",     "ALTER TABLE prestamos ADD COLUMN IF NOT EXISTS cuota_mensual DECIMAL"),
            ("total_pagar",       "ALTER TABLE prestamos ADD COLUMN IF NOT EXISTS total_pagar DECIMAL"),
        ]:
            cur.execute(ddl)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS inversiones (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES usuarios(id),
                nombre TEXT,
                monto DECIMAL,
                roi_esperado DECIMAL,
                fecha DATE
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ahorros (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES usuarios(id),
                meta_nombre TEXT,
                objetivo DECIMAL,
                actual DECIMAL DEFAULT 0
            )
        """)

        cur.execute("SELECT 1 FROM usuarios WHERE username = 'admin'")
        if not cur.fetchone():
            hashed = bcrypt.hashpw("admin123".encode(), bcrypt.gensalt()).decode()
            cur.execute(
                "INSERT INTO usuarios (username, password, role) VALUES ('admin', %s, 'admin')",
                (hashed,)
            )

        conn.commit()
    except Exception as e:
        conn.rollback()
        st.error(f"Error al inicializar tablas: {e}")
    finally:
        cur.close()

# =========================================================
# 4. CONFIGURACIÓN + CSS PREMIUM (FinTech World Class)
# =========================================================
st.set_page_config(page_title="Finanzas Pro PY", layout="wide", page_icon="📈")
init_db()

# --- CSS Premium ----------------------------------------------------------
# Se usa <style>...</style> con triple-single-quote para evitar que Streamlit
# escape/traduzca el contenido (bug visto cuando se enviaba como texto).
_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

:root {
  --brand:  #1E88E5;
  --brand2: #42A5F5;
  --card:   #0D0D0D;
  --card2:  #161616;
  --txt:    #FFFFFF;
  --muted:  #9BA3AF;
  --ok:     #10B981;
  --border: #242424;
  --shadow: 0 4px 24px rgba(0,0,0,0.6);
}

html, body, [class*="css"] {
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
  color: var(--txt) !important;
}

.stApp { background: #000000 !important; }

p, span, div, label, li, td, th, caption, figcaption { color: var(--txt) !important; }

h1, h2, h3, h4, h5, h6 {
  font-weight: 700 !important;
  letter-spacing: -0.02em;
  color: #FFFFFF !important;
}

section[data-testid="stSidebar"] {
  background: #0A0A0A !important;
  border-right: 1px solid var(--border) !important;
}
section[data-testid="stSidebar"] * { color: #fff !important; }

.stTabs [data-baseweb="tab-list"] { gap: 6px; background: transparent; border-bottom: 1px solid var(--border); }
.stTabs [data-baseweb="tab"] {
  background: var(--card2); border-radius: 12px 12px 0 0; padding: 10px 20px;
  font-weight: 600; color: var(--muted) !important;
  border: 1px solid var(--border); border-bottom: none;
}
.stTabs [aria-selected="true"] { background: var(--brand) !important; color: #fff !important; }

[data-testid="stMetric"] {
  background: var(--card2) !important;
  padding: 18px 20px; border-radius: 15px;
  border: 1px solid var(--border) !important;
  box-shadow: var(--shadow);
  transition: transform .15s ease, box-shadow .15s ease;
}
[data-testid="stMetric"]:hover {
  transform: translateY(-2px);
  box-shadow: 0 8px 32px rgba(30,136,229,0.18);
}
[data-testid="stMetricLabel"] { color: var(--muted) !important; font-weight: 600; font-size: .82rem; text-transform: uppercase; letter-spacing: .5px; }
[data-testid="stMetricValue"] { color: var(--brand2) !important; font-weight: 800 !important; font-size: 1.6rem !important; }

.stButton > button {
  width: 100%; border-radius: 12px; height: 3em; border: none;
  background: linear-gradient(135deg, var(--brand) 0%, var(--brand2) 100%);
  color: #fff !important; font-weight: 600; letter-spacing: .3px;
  box-shadow: 0 4px 14px rgba(30,136,229,.3);
  transition: all .15s ease;
}
.stButton > button:hover { transform: translateY(-1px); box-shadow: 0 6px 22px rgba(66,165,245,.4); filter: brightness(1.08); }
.stButton > button:active { transform: translateY(0); }

.stTextInput input, .stNumberInput input, .stDateInput input,
div[data-baseweb="select"] > div, textarea {
  border-radius: 10px !important;
  border: 1px solid var(--border) !important;
  background: #111111 !important;
  color: #FFFFFF !important;
}
.stTextInput input::placeholder, .stNumberInput input::placeholder { color: var(--muted) !important; }

[data-baseweb="popover"] li, [data-baseweb="menu"] li { background: #111111 !important; color: #FFFFFF !important; }
[data-baseweb="popover"] li:hover, [data-baseweb="menu"] li:hover { background: #1E88E5 !important; }

.streamlit-expanderHeader, [data-testid="stExpander"] {
  background: var(--card2) !important;
  border-radius: 12px !important;
  border: 1px solid var(--border) !important;
  box-shadow: var(--shadow);
}
details summary { color: var(--txt) !important; }

[data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; border: 1px solid var(--border); box-shadow: var(--shadow); }
[data-testid="stDataFrame"] th { background: #111111 !important; color: var(--muted) !important; }
[data-testid="stDataFrame"] td { background: #000000 !important; color: #FFFFFF !important; }

.stProgress > div > div > div { background: linear-gradient(90deg, var(--brand), var(--ok)) !important; border-radius: 10px; }

.stAlert { border-radius: 12px !important; border-left-width: 4px !important; }

hr { border-color: var(--border) !important; }

.stCaption, [data-testid="stCaption"] { color: var(--muted) !important; }

.stCheckbox label span { color: var(--txt) !important; }
.stRadio label span { color: var(--txt) !important; }

label, [data-testid="stWidgetLabel"] p { color: #FFFFFF !important; font-weight: 600 !important; }

[data-testid="stForm"] {
  background: var(--card2) !important;
  border: 1px solid var(--border) !important;
  border-radius: 14px !important;
  padding: 1rem !important;
}

.login-wrap {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  min-height: 30vh;
  padding: 2rem 1rem;
}
.login-card {
  background: #0D0D0D;
  border: 1px solid #2a2a2a;
  border-radius: 20px;
  padding: 2.5rem 2.2rem 2rem 2.2rem;
  width: 100%;
  max-width: 420px;
  box-shadow: 0 8px 40px rgba(0,0,0,0.8);
  text-align: center;
}
.login-card h1 {
  font-size: 1.75rem;
  font-weight: 800;
  color: #FFFFFF !important;
  margin: 0 0 .3rem 0;
}
.login-card p {
  font-size: .85rem;
  color: #9BA3AF !important;
  margin: 0 0 1.8rem 0;
}

@media (max-width: 768px) {
  .block-container { padding: 1rem 0.8rem !important; }
  .login-card { padding: 1.8rem 1.2rem; border-radius: 16px; }
  .login-card h1 { font-size: 1.4rem; }
  .stTextInput input, .stNumberInput input { font-size: 16px !important; height: 3rem !important; }
  .stButton > button { height: 3.2em !important; font-size: 1rem !important; }
}

/* Ocultar barra superior, footer y boton deploy de Streamlit Cloud */
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
header { visibility: hidden; }
.stAppDeployButton { display: none; }

/* Eliminar hueco superior que deja el header oculto */
.block-container { padding-top: 1rem !important; }
</style>
"""
st.markdown(_CSS, unsafe_allow_html=True)

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

# =========================================================
# 5. PANTALLA DE ACCESO
# =========================================================
if not st.session_state.logged_in:
    st.markdown("""
    <div class="login-wrap">
      <div class="login-card">
        <h1>🚀 Finanzas Pro PY</h1>
        <p>Acceso restringido &middot; Solo usuarios autorizados</p>
      </div>
    </div>
    """, unsafe_allow_html=True)

    _, col_c, _ = st.columns([1, 2, 1])
    with col_c:
        u = st.text_input("Usuario", key="l_u")
        p = st.text_input("Contraseña", type="password", key="l_p")
        if st.button("🔐 Entrar", use_container_width=True):
            conn, cur = get_cursor()
            try:
                cur.execute("SELECT * FROM usuarios WHERE username = %s", (u,))
                user = cur.fetchone()
                if user and bcrypt.checkpw(p.encode(), user['password'].encode()):
                    st.session_state.logged_in = True
                    st.session_state.user_id = user['id']
                    st.session_state.username = user['username']
                    st.session_state.role = user['role'] if 'role' in user.keys() else 'user'
                    st.rerun()
                else:
                    st.error("❌ Credenciales incorrectas")
            finally:
                cur.close()
        st.caption("Sin cuenta? Pedile al administrador que te registre.")

# =========================================================
# 6. DASHBOARD
# =========================================================
else:
    # Asegurarse de tener role (compatibilidad sesiones antiguas)
    if "role" not in st.session_state:
        conn_r, cur_r = get_cursor()
        try:
            cur_r.execute("SELECT role FROM usuarios WHERE id = %s", (st.session_state.user_id,))
            row_r = cur_r.fetchone()
            st.session_state.role = (row_r['role'] if row_r else 'user') or 'user'
        finally:
            cur_r.close()

    is_admin = st.session_state.get("role", "user") == "admin"

    st.sidebar.title(f"👋 {st.session_state.username}")
    rol_badge = "🛡️ Administrador" if is_admin else "👤 Usuario"
    st.sidebar.markdown(f"**{rol_badge}**")
    st.sidebar.divider()
    if st.sidebar.button("Cerrar Sesión"):
        for k in ["logged_in", "user_id", "username", "role"]:
            st.session_state.pop(k, None)
        st.rerun()

    # Pestañas: el admin ve una adicional "👥 Usuarios"
    tabs_labels = ["💰 Movimientos", "🤝 Préstamos", "📈 Inversiones", "🎯 Ahorros"]
    if is_admin:
        tabs_labels.append("👥 Usuarios")
    menu = st.tabs(tabs_labels)

    # -----------------------------------------------------
    # PESTAÑA 1: MOVIMIENTOS (DASHBOARD + PLOTLY)
    # -----------------------------------------------------
    with menu[0]:
        st.header("💰 Gestión de Caja")

        # -------- DASHBOARD KPIs --------
        conn, cur = get_cursor()
        try:
            cur.execute("""
                SELECT
                    COALESCE(SUM(CASE WHEN tipo='Ingreso' THEN monto ELSE 0 END),0) AS ingresos,
                    COALESCE(SUM(CASE WHEN tipo='Egreso'  THEN monto ELSE 0 END),0) AS egresos
                FROM movimientos WHERE user_id = %s
            """, (st.session_state.user_id,))
            tot = cur.fetchone()
            ingresos_tot = Decimal(str(tot['ingresos'] or 0))
            egresos_tot = Decimal(str(tot['egresos'] or 0))
            saldo_neto = ingresos_tot - egresos_tot

            k1, k2, k3 = st.columns(3)
            k1.metric("💵 Ingresos Totales", fmt_gs(ingresos_tot))
            k2.metric("💸 Egresos Totales", fmt_gs(egresos_tot))
            k3.metric(
                "📊 Saldo Neto",
                fmt_gs(saldo_neto),
                delta=f"{'▲' if saldo_neto >= 0 else '▼'} {fmt_gs(abs(saldo_neto))}",
                delta_color="normal" if saldo_neto >= 0 else "inverse",
            )

            st.divider()

            # -------- FORMULARIO NUEVO MOVIMIENTO --------
            CATEGORIAS_EGRESO = ["Alimentación", "Transporte", "Vivienda", "Servicios",
                                 "Salud", "Educación", "Ocio", "Ropa", "Otros"]
            CATEGORIAS_INGRESO = ["Sueldo", "Freelance", "Ventas", "Intereses", "Otros"]

            with st.expander("➕ Registrar Nuevo Movimiento", expanded=False):
                col1, col2, col3 = st.columns(3)
                with col1:
                    tipo = st.selectbox("Tipo", ["Ingreso", "Egreso"])
                    cats = CATEGORIAS_INGRESO if tipo == "Ingreso" else CATEGORIAS_EGRESO
                    categoria = st.selectbox("Categoría", cats)
                with col2:
                    monto = st.number_input("Monto (Gs.)", min_value=0, step=5000)
                    fecha = st.date_input("Fecha", datetime.now())
                with col3:
                    desc = st.text_input("Descripción")
                    st.write(""); st.write("")  # espaciado
                    if st.button("💾 Guardar Movimiento", use_container_width=True):
                        if monto <= 0:
                            st.warning("El monto debe ser mayor a 0.")
                        else:
                            try:
                                cur.execute(
                                    "INSERT INTO movimientos (user_id, tipo, monto, descripcion, fecha, categoria) VALUES (%s,%s,%s,%s,%s,%s)",
                                    (st.session_state.user_id, tipo, monto, desc, fecha, categoria)
                                )
                                conn.commit()
                                st.success("¡Movimiento registrado!")
                                st.rerun()
                            except Exception as e:
                                conn.rollback()
                                st.error(f"Error: {e}")

            # -------- GRÁFICO TORTA EGRESOS --------
            cur.execute("""
                SELECT COALESCE(categoria,'Otros') AS categoria, SUM(monto) AS total
                FROM movimientos
                WHERE user_id = %s AND tipo = 'Egreso'
                GROUP BY categoria ORDER BY total DESC
            """, (st.session_state.user_id,))
            egresos_cat = cur.fetchall()

            col_g, col_t = st.columns([3, 2])

            with col_g:
                st.subheader("🍩 Egresos por Categoría")
                if egresos_cat:
                    df_cat = pd.DataFrame([dict(r) for r in egresos_cat])
                    df_cat["total"] = df_cat["total"].astype(float)
                    fig = px.pie(
                        df_cat, names="categoria", values="total",
                        hole=0.55,
                        color_discrete_sequence=px.colors.sequential.Blues_r
                    )
                    fig.update_traces(
                        textposition="outside",
                        textinfo="label+percent",
                        hovertemplate="<b>%{label}</b><br>%{value:,.0f} Gs.<extra></extra>"
                    )
                    fig.update_layout(
                        showlegend=False,
                        margin=dict(l=10, r=10, t=10, b=10),
                        height=380,
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        font=dict(family="Inter", size=13, color="#FFFFFF"),
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Registrá al menos un egreso para ver el gráfico.")

            with col_t:
                st.subheader("📋 Últimos Movimientos")
                cur.execute("""
                    SELECT fecha, tipo, COALESCE(categoria,'-') AS categoria, monto, descripcion
                    FROM movimientos WHERE user_id = %s
                    ORDER BY fecha DESC, id DESC LIMIT 10
                """, (st.session_state.user_id,))
                ultimos = cur.fetchall()
                if ultimos:
                    df_u = pd.DataFrame([dict(r) for r in ultimos])
                    df_u["monto"] = df_u["monto"].apply(lambda v: fmt_gs(v))
                    st.dataframe(df_u, use_container_width=True, hide_index=True)
                else:
                    st.info("Sin movimientos todavía.")
        finally:
            cur.close()

    # -----------------------------------------------------
    # PESTAÑA 2: PRÉSTAMOS P2P (REFACTORIZADA + DASHBOARD)
    # -----------------------------------------------------
    with menu[1]:
        st.header("🤝 Préstamos P2P — Motor Financiero PY")
        st.caption("Cálculos con precisión Decimal · Sistema Francés · Redondeo a Gs. entero · IVA 10% informativo")

        conn, cur = get_cursor()
        try:
            # -------- DASHBOARD KPIs (Total a Cobrar vs Total a Pagar) --------
            cur.execute("""
                SELECT
                    COALESCE(SUM(CASE WHEN prestamista_id = %(uid)s THEN total_pagar ELSE 0 END),0) AS por_cobrar,
                    COALESCE(SUM(CASE WHEN prestatario_id = %(uid)s THEN total_pagar ELSE 0 END),0) AS por_pagar,
                    COUNT(*) FILTER (WHERE estado='aprobado') AS activos,
                    COUNT(*) FILTER (WHERE estado='pendiente') AS pendientes
                FROM prestamos
                WHERE (prestamista_id = %(uid)s OR prestatario_id = %(uid)s)
                  AND estado = 'aprobado'
            """, {"uid": st.session_state.user_id})
            kp = cur.fetchone() or {}
            por_cobrar = Decimal(str(kp['por_cobrar'] or 0))
            por_pagar = Decimal(str(kp['por_pagar'] or 0))
            balance = por_cobrar - por_pagar

            # Pendientes (no aprobados) — contador separado
            cur.execute("""
                SELECT COUNT(*) AS n FROM prestamos
                WHERE (prestamista_id = %(uid)s OR prestatario_id = %(uid)s)
                  AND estado = 'pendiente'
            """, {"uid": st.session_state.user_id})
            n_pend = (cur.fetchone() or {}).get('n', 0) or 0

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("💚 Total a Cobrar", fmt_gs(por_cobrar))
            k2.metric("❤️ Total a Pagar", fmt_gs(por_pagar))
            k3.metric(
                "⚖️ Balance Neto", fmt_gs(balance),
                delta=("Acreedor neto" if balance >= 0 else "Deudor neto"),
                delta_color="normal" if balance >= 0 else "inverse",
            )
            k4.metric("📬 Pendientes", f"{n_pend}")

            st.divider()

            cur.execute(
                "SELECT id, username FROM usuarios WHERE id != %s ORDER BY username",
                (st.session_state.user_id,)
            )
            usuarios_dict = {u['username']: u['id'] for u in cur.fetchall()}

            if not usuarios_dict:
                st.info("📭 Aún no hay otros usuarios registrados para operar. Invita a alguien a crear cuenta.")
            else:
                col_f, col_s = st.columns(2)

                # ---------------- NUEVA PROPUESTA ----------------
                with col_f:
                    st.subheader("📝 Nueva Propuesta")

                    # Inputs FUERA del form → permiten simulación en vivo
                    socio = st.selectbox("Elegir Socio", list(usuarios_dict.keys()))
                    mon = st.number_input(
                        "Monto Capital (Gs.)", min_value=0, step=100000, value=1000000
                    )
                    col_t, col_p = st.columns(2)
                    with col_t:
                        inte = st.number_input(
                            "Tasa mensual (%)", min_value=0.0, max_value=50.0,
                            value=5.0, step=0.25,
                            help="Tasa nominal mensual. Ej: 5% mensual ≈ 79.6% anual efectivo."
                        )
                    with col_p:
                        plazo = st.number_input(
                            "Plazo (meses)", min_value=1, max_value=120, value=12, step=1
                        )
                    sistema = st.selectbox(
                        "Sistema de amortización",
                        ["Francés", "Alemán", "Simple"],
                        help="Francés: cuota fija (estándar BCP). Alemán: capital fijo. Simple: sin capitalización."
                    )
                    rol = st.radio("Acción", ["Le voy a prestar", "Le pido prestado"], horizontal=True)

                    # ---------- SIMULADOR EN VIVO ----------
                    if mon > 0 and plazo > 0:
                        sim = calcular_prestamo(mon, inte, plazo, sistema)
                        if sim:
                            st.markdown("##### 🧮 Simulación")
                            m1, m2, m3 = st.columns(3)
                            m1.metric("Cuota mensual", fmt_gs(sim["cuota_promedio"]))
                            m2.metric("Total a pagar", fmt_gs(sim["total_pagar"]))
                            m3.metric("Intereses", fmt_gs(sim["total_intereses"]))
                            m4, m5, m6 = st.columns(3)
                            m4.metric("TEA", f"{sim['tea']}%", help="Tasa Efectiva Anual")
                            m5.metric("CET anual", f"{sim['cet_anual']}%", help="Costo Efectivo Total (BCP)")
                            m6.metric("IVA s/intereses", fmt_gs(sim["iva_intereses"]))

                            with st.expander("📅 Ver cronograma de amortización"):
                                df_cron = pd.DataFrame(sim["cronograma"])
                                st.dataframe(df_cron, use_container_width=True, hide_index=True)

                    # ---------- ENVIAR ----------
                    if st.button("📤 Enviar Solicitud", use_container_width=True, type="primary"):
                        # Validaciones
                        if mon <= 0:
                            st.error("El monto debe ser mayor a 0.")
                        elif plazo <= 0:
                            st.error("El plazo debe ser mayor a 0.")
                        elif socio not in usuarios_dict:
                            st.error("Socio inválido.")
                        elif usuarios_dict[socio] == st.session_state.user_id:
                            st.error("No podés prestarte a vos mismo.")
                        else:
                            t_id = usuarios_dict[socio]
                            p_id = st.session_state.user_id if rol == "Le voy a prestar" else t_id
                            b_id = t_id if rol == "Le voy a prestar" else st.session_state.user_id
                            sim = calcular_prestamo(mon, inte, plazo, sistema)
                            venc = date.today() + relativedelta(months=int(plazo))
                            try:
                                cur.execute("""
                                    INSERT INTO prestamos
                                      (prestamista_id, prestatario_id, monto, interes,
                                       plazo_meses, sistema, fecha_vencimiento,
                                       cuota_mensual, total_pagar)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """, (
                                    p_id, b_id, mon, inte,
                                    int(plazo), sistema, venc,
                                    int(sim["cuota_promedio"]), int(sim["total_pagar"])
                                ))
                                conn.commit()
                                st.success("✅ Propuesta enviada con cronograma calculado.")
                                st.rerun()
                            except Exception as e:
                                conn.rollback()
                                st.error(f"Error al enviar propuesta: {e}")

                # ---------------- BANDEJA DE ENTRADA ----------------
                with col_s:
                    st.subheader("📥 Bandeja de Entrada")
                    # JOIN corregido: usamos CASE para traer al OTRO usuario sin duplicar filas.
                    cur.execute("""
                        SELECT
                            p.id,
                            p.monto,
                            p.interes,
                            p.plazo_meses,
                            p.sistema,
                            p.cuota_mensual,
                            p.total_pagar,
                            p.fecha_vencimiento,
                            CASE
                                WHEN p.prestamista_id = %(uid)s THEN 'Prestás a'
                                ELSE 'Te presta'
                            END AS rol,
                            (SELECT username FROM usuarios
                             WHERE id = CASE
                                WHEN p.prestamista_id = %(uid)s THEN p.prestatario_id
                                ELSE p.prestamista_id
                             END) AS contraparte
                        FROM prestamos p
                        WHERE (p.prestamista_id = %(uid)s OR p.prestatario_id = %(uid)s)
                          AND p.estado = 'pendiente'
                        ORDER BY p.fecha_creacion DESC
                    """, {"uid": st.session_state.user_id})

                    pendientes = cur.fetchall()
                    if not pendientes:
                        st.info("No tienes propuestas pendientes.")
                    else:
                        for p_rec in pendientes:
                            titulo = f"{p_rec['rol']} {p_rec['contraparte']} — {fmt_gs(p_rec['monto'])}"
                            with st.expander(titulo):
                                st.write(f"**Tasa:** {p_rec['interes']}% mensual")
                                st.write(f"**Plazo:** {p_rec['plazo_meses']} meses ({p_rec['sistema']})")
                                if p_rec['cuota_mensual']:
                                    st.write(f"**Cuota estimada:** {fmt_gs(p_rec['cuota_mensual'])}")
                                if p_rec['total_pagar']:
                                    st.write(f"**Total a pagar:** {fmt_gs(p_rec['total_pagar'])}")
                                if p_rec['fecha_vencimiento']:
                                    st.write(f"**Vencimiento:** {p_rec['fecha_vencimiento']}")

                                c1, c2 = st.columns(2)
                                if c1.button("✅ Aprobar", key=f"a_{p_rec['id']}", use_container_width=True):
                                    try:
                                        cur.execute(
                                            "UPDATE prestamos SET estado = 'aprobado' WHERE id = %s",
                                            (p_rec['id'],)
                                        )
                                        conn.commit()
                                        st.rerun()
                                    except Exception as e:
                                        conn.rollback()
                                        st.error(f"Error: {e}")
                                if c2.button("❌ Rechazar", key=f"r_{p_rec['id']}", use_container_width=True):
                                    try:
                                        cur.execute(
                                            "UPDATE prestamos SET estado = 'rechazado' WHERE id = %s",
                                            (p_rec['id'],)
                                        )
                                        conn.commit()
                                        st.rerun()
                                    except Exception as e:
                                        conn.rollback()
                                        st.error(f"Error: {e}")

            # ---------------- PRÉSTAMOS ACTIVOS ----------------
            st.divider()
            st.subheader("📈 Préstamos Activos")
            cur.execute("""
                SELECT
                    u1.username AS prestamista,
                    u2.username AS prestatario,
                    p.monto,
                    p.interes AS tasa_mensual_pct,
                    p.plazo_meses,
                    p.sistema,
                    p.cuota_mensual,
                    p.total_pagar,
                    p.fecha_vencimiento
                FROM prestamos p
                JOIN usuarios u1 ON p.prestamista_id = u1.id
                JOIN usuarios u2 ON p.prestatario_id = u2.id
                WHERE (p.prestamista_id = %(uid)s OR p.prestatario_id = %(uid)s)
                  AND p.estado = 'aprobado'
                ORDER BY p.fecha_vencimiento NULLS LAST
            """, {"uid": st.session_state.user_id})
            rows = cur.fetchall()
            if rows:
                df_l = pd.DataFrame([dict(r) for r in rows])
                st.dataframe(df_l, use_container_width=True, hide_index=True)
            else:
                st.info("No tienes préstamos activos.")

        finally:
            cur.close()

    # -----------------------------------------------------
    # PESTAÑA 3: INVERSIONES (GGR + PROYECCIÓN 12 MESES)
    # -----------------------------------------------------
    with menu[2]:
        st.header("📈 Seguimiento de Inversiones")
        st.caption("GGR (Gross Growth Rate) mensual equivalente a partir del ROI anual · Proyección de valor a 12 meses")

        conn, cur = get_cursor()
        try:
            # -------- KPIs portfolio --------
            cur.execute("""
                SELECT
                    COALESCE(SUM(monto),0) AS capital,
                    COALESCE(SUM(monto * roi_esperado / 100.0),0) AS ganancia_anual_esperada,
                    COUNT(*) AS n
                FROM inversiones WHERE user_id = %s
            """, (st.session_state.user_id,))
            t = cur.fetchone() or {}
            capital_tot = Decimal(str(t['capital'] or 0))
            gan_anual = Decimal(str(t['ganancia_anual_esperada'] or 0))
            valor_12m = capital_tot + gan_anual
            n_activos = t['n'] or 0

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("💼 Capital Invertido", fmt_gs(capital_tot))
            k2.metric("📈 Ganancia Anual Est.", fmt_gs(gan_anual))
            k3.metric("🎯 Valor Proyectado 12m", fmt_gs(valor_12m))
            k4.metric("🧾 Activos", f"{n_activos}")

            st.divider()

            # -------- FORMULARIO REGISTRO --------
            with st.expander("➕ Registrar Nuevo Activo", expanded=(n_activos == 0)):
                with st.form("inv_form"):
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        n_inv = st.text_input("Nombre de la Inversión")
                    with c2:
                        m_inv = st.number_input("Capital Invertido (Gs.)", min_value=0, step=100000)
                    with c3:
                        roi_inv = st.number_input("ROI Anual Estimado (%)", min_value=0.0, step=0.5, value=10.0)
                    if st.form_submit_button("💾 Registrar", use_container_width=True):
                        if not n_inv or m_inv <= 0:
                            st.warning("Completa nombre y monto > 0.")
                        else:
                            try:
                                cur.execute(
                                    "INSERT INTO inversiones (user_id, nombre, monto, roi_esperado, fecha) VALUES (%s, %s, %s, %s, %s)",
                                    (st.session_state.user_id, n_inv, m_inv, roi_inv, datetime.now().date())
                                )
                                conn.commit()
                                st.success("Inversión guardada")
                                st.rerun()
                            except Exception as e:
                                conn.rollback()
                                st.error(f"Error: {e}")

            # -------- TABLA CON GGR Y PROYECCIÓN --------
            cur.execute(
                "SELECT id, nombre, monto, roi_esperado, fecha FROM inversiones WHERE user_id = %s ORDER BY fecha DESC",
                (st.session_state.user_id,)
            )
            invs = cur.fetchall()

            if not invs:
                st.info("Aún no registraste inversiones. Añadí un activo para ver GGR y proyecciones.")
            else:
                st.subheader("📊 Portfolio con GGR y Proyección 12 meses")
                filas = []
                for r in invs:
                    capital = Decimal(str(r['monto']))
                    roi_anual = Decimal(str(r['roi_esperado'] or 0)) / Decimal("100")
                    # GGR mensual compuesto equivalente: (1+roi_anual)^(1/12) - 1
                    if roi_anual > -1:
                        ggr_mensual = (Decimal("1") + roi_anual) ** (Decimal("1") / Decimal("12")) - Decimal("1")
                    else:
                        ggr_mensual = Decimal("0")
                    valor_12 = capital * (Decimal("1") + roi_anual)
                    ganancia = valor_12 - capital
                    filas.append({
                        "Activo": r['nombre'],
                        "Capital": fmt_gs(capital),
                        "ROI Anual": f"{roi_anual*100:.2f}%",
                        "GGR Mensual": f"{ggr_mensual*100:.3f}%",
                        "Valor a 12 m.": fmt_gs(valor_12),
                        "Ganancia": fmt_gs(ganancia),
                        "Fecha": r['fecha'],
                    })
                df_inv = pd.DataFrame(filas)
                st.dataframe(df_inv, use_container_width=True, hide_index=True)

                # -------- GRÁFICO PROYECCIÓN MENSUAL ACUMULADA --------
                st.subheader("📉 Proyección de valor (12 meses)")
                meses = list(range(0, 13))
                fig = go.Figure()
                for r in invs:
                    capital = float(r['monto'])
                    roi_anual = float(r['roi_esperado'] or 0) / 100.0
                    ggr_m = (1 + roi_anual) ** (1 / 12) - 1
                    valores = [capital * ((1 + ggr_m) ** m) for m in meses]
                    fig.add_trace(go.Scatter(
                        x=meses, y=valores, mode="lines+markers",
                        name=r['nombre'],
                        hovertemplate="Mes %{x}<br>%{y:,.0f} Gs.<extra></extra>",
                        line=dict(width=3),
                    ))
                fig.update_layout(
                    xaxis_title="Mes", yaxis_title="Valor proyectado (Gs.)",
                    height=420, margin=dict(l=10, r=10, t=10, b=10),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    font=dict(family="Inter", color="#FFFFFF"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                                font=dict(color="#FFFFFF")),
                    hovermode="x unified",
                )
                fig.update_xaxes(showgrid=True, gridcolor="#2A2A2A", color="#FFFFFF")
                fig.update_yaxes(showgrid=True, gridcolor="#2A2A2A", color="#FFFFFF", tickformat=",.0f")
                st.plotly_chart(fig, use_container_width=True)
        finally:
            cur.close()

    # -----------------------------------------------------
    # PESTAÑA 4: AHORROS (DASHBOARD + APORTE PERSONALIZADO)
    # -----------------------------------------------------
    with menu[3]:
        st.header("🎯 Metas de Ahorro")

        conn, cur = get_cursor()
        try:
            # -------- KPIs globales --------
            cur.execute("""
                SELECT COALESCE(SUM(actual),0) AS ahorrado,
                       COALESCE(SUM(objetivo),0) AS objetivo,
                       COUNT(*) AS n
                FROM ahorros WHERE user_id = %s
            """, (st.session_state.user_id,))
            t = cur.fetchone() or {}
            ahorrado = Decimal(str(t['ahorrado'] or 0))
            objetivo = Decimal(str(t['objetivo'] or 0))
            faltante = max(objetivo - ahorrado, Decimal("0"))
            pct_global = float(ahorrado / objetivo) if objetivo > 0 else 0.0
            pct_global = min(pct_global, 1.0)

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("💰 Ahorrado Total", fmt_gs(ahorrado))
            k2.metric("🏁 Objetivo Total", fmt_gs(objetivo))
            k3.metric("📉 Falta", fmt_gs(faltante))
            k4.metric("📊 Progreso Global", f"{pct_global*100:.1f}%")

            st.markdown("**Progreso general**")
            st.progress(pct_global)

            st.divider()

            # -------- CREAR NUEVA META --------
            with st.expander("🎯 Crear Nueva Meta"):
                with st.form("ah_form"):
                    c1, c2 = st.columns(2)
                    with c1:
                        n_meta = st.text_input("¿Para qué estás ahorrando?")
                    with c2:
                        obj_meta = st.number_input("Objetivo Final (Gs.)", min_value=0, step=100000)
                    if st.form_submit_button("Crear Meta", use_container_width=True):
                        if not n_meta or obj_meta <= 0:
                            st.warning("Completa nombre y objetivo > 0.")
                        else:
                            try:
                                cur.execute(
                                    "INSERT INTO ahorros (user_id, meta_nombre, objetivo) VALUES (%s, %s, %s)",
                                    (st.session_state.user_id, n_meta, obj_meta)
                                )
                                conn.commit()
                                st.rerun()
                            except Exception as e:
                                conn.rollback()
                                st.error(f"Error: {e}")

            # -------- LISTA DE METAS --------
            cur.execute(
                "SELECT id, meta_nombre, objetivo, actual FROM ahorros WHERE user_id = %s ORDER BY id",
                (st.session_state.user_id,)
            )
            metas = cur.fetchall()

            if not metas:
                st.info("🗒️ Todavía no creaste metas de ahorro. Empezá creando la primera arriba.")
            else:
                for m in metas:
                    objetivo_m = float(m['objetivo']) if m['objetivo'] else 0
                    actual_m = float(m['actual']) if m['actual'] else 0
                    porcentaje = (actual_m / objetivo_m) if objetivo_m > 0 else 0
                    porcentaje_c = min(porcentaje, 1.0)
                    completo = porcentaje >= 1.0

                    st.markdown(f"### {'🏆 ' if completo else '🎯 '}{m['meta_nombre']}")
                    c_inf, c_bar = st.columns([1, 3])
                    with c_inf:
                        st.metric("Progreso", f"{porcentaje*100:.1f}%")
                    with c_bar:
                        st.write(f"**{fmt_gs(actual_m)}** / {fmt_gs(objetivo_m)}  ·  falta **{fmt_gs(max(objetivo_m-actual_m,0))}**")
                        st.progress(porcentaje_c)

                    # ---- Aportar ----
                    c_qa, c_qb, c_qc, c_custom = st.columns([1, 1, 1, 2])
                    aportes_rapidos = [50000, 100000, 500000]
                    for idx, apq in enumerate(aportes_rapidos):
                        col_ref = [c_qa, c_qb, c_qc][idx]
                        if col_ref.button(f"+ {fmt_gs(apq)}", key=f"q{apq}_{m['id']}", use_container_width=True):
                            try:
                                cur.execute(
                                    "UPDATE ahorros SET actual = actual + %s WHERE id = %s AND user_id = %s",
                                    (apq, m['id'], st.session_state.user_id)
                                )
                                conn.commit()
                                st.rerun()
                            except Exception as e:
                                conn.rollback()
                                st.error(f"Error: {e}")

                    with c_custom:
                        with st.form(f"aporte_custom_{m['id']}", clear_on_submit=True):
                            cc1, cc2 = st.columns([2, 1])
                            with cc1:
                                monto_custom = st.number_input(
                                    "Aporte personalizado (Gs.)",
                                    min_value=0, step=10000, key=f"mc_{m['id']}"
                                )
                            with cc2:
                                st.write(""); st.write("")
                                enviar = st.form_submit_button("💵 Aportar", use_container_width=True)
                            if enviar:
                                if monto_custom <= 0:
                                    st.warning("Ingresá un monto > 0.")
                                else:
                                    try:
                                        cur.execute(
                                            "UPDATE ahorros SET actual = actual + %s WHERE id = %s AND user_id = %s",
                                            (monto_custom, m['id'], st.session_state.user_id)
                                        )
                                        conn.commit()
                                        st.success(f"Aporte de {fmt_gs(monto_custom)} registrado.")
                                        st.rerun()
                                    except Exception as e:
                                        conn.rollback()
                                        st.error(f"Error: {e}")

                    st.divider()
        finally:
            cur.close()

    # -----------------------------------------------------
    # PESTAÑA 5: USUARIOS (SÓLO ADMIN)
    # -----------------------------------------------------
    if is_admin:
        with menu[4]:
            st.header("👥 Gestión de Usuarios")
            st.caption("Panel de administración · Sólo los administradores pueden crear, editar o eliminar usuarios.")

            conn, cur = get_cursor()
            try:
                # -------- KPIs --------
                cur.execute("""
                    SELECT
                        COUNT(*) AS total,
                        COUNT(*) FILTER (WHERE role='admin') AS admins,
                        COUNT(*) FILTER (WHERE role='user' OR role IS NULL) AS users
                    FROM usuarios
                """)
                stats = cur.fetchone() or {}
                k1, k2, k3 = st.columns(3)
                k1.metric("👥 Total Usuarios", stats.get('total', 0) or 0)
                k2.metric("🛡️ Administradores", stats.get('admins', 0) or 0)
                k3.metric("👤 Usuarios Estándar", stats.get('users', 0) or 0)

                st.divider()

                # -------- CREAR NUEVO USUARIO --------
                with st.expander("➕ Crear Nuevo Usuario", expanded=False):
                    with st.form("create_user_form", clear_on_submit=True):
                        cu1, cu2 = st.columns(2)
                        with cu1:
                            new_username = st.text_input("Nombre de usuario", key="adm_new_u")
                            new_role = st.selectbox(
                                "Rol", ["user", "admin"],
                                format_func=lambda r: "🛡️ Administrador" if r == "admin" else "👤 Usuario",
                                key="adm_new_r"
                            )
                        with cu2:
                            new_password = st.text_input("Contraseña", type="password", key="adm_new_p")
                            new_password2 = st.text_input("Confirmar contraseña", type="password", key="adm_new_p2")

                        submitted = st.form_submit_button("✅ Crear Usuario", use_container_width=True)
                        if submitted:
                            if not new_username or not new_password:
                                st.warning("Completá usuario y contraseña.")
                            elif len(new_password) < 6:
                                st.warning("La contraseña debe tener al menos 6 caracteres.")
                            elif new_password != new_password2:
                                st.error("Las contraseñas no coinciden.")
                            else:
                                try:
                                    hashed = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()
                                    cur.execute(
                                        "INSERT INTO usuarios (username, password, role) VALUES (%s, %s, %s)",
                                        (new_username.strip(), hashed, new_role)
                                    )
                                    conn.commit()
                                    st.success(f"✅ Usuario '{new_username}' creado como {new_role}.")
                                    st.rerun()
                                except psycopg2.errors.UniqueViolation:
                                    conn.rollback()
                                    st.error("❌ Ese nombre de usuario ya existe.")
                                except Exception as e:
                                    conn.rollback()
                                    st.error(f"Error: {e}")

                # -------- LISTA DE USUARIOS --------
                st.subheader("📋 Lista de Usuarios")
                cur.execute("""
                    SELECT id, username, COALESCE(role,'user') AS role
                    FROM usuarios
                    ORDER BY (role = 'admin') DESC, username ASC
                """)
                users = cur.fetchall()

                for usr in users:
                    es_self = usr['id'] == st.session_state.user_id
                    icono = "🛡️" if usr['role'] == 'admin' else "👤"
                    etiqueta = f"{icono} **{usr['username']}** — `{usr['role']}`"
                    if es_self:
                        etiqueta += " *(vos)*"

                    with st.expander(etiqueta):
                        st.write(f"**ID:** {usr['id']}")
                        st.write(f"**Rol actual:** {usr['role']}")

                        ca, cb, cc = st.columns(3)

                        # --- Cambiar rol ---
                        with ca:
                            nuevo_rol = st.selectbox(
                                "Cambiar rol",
                                ["user", "admin"],
                                index=(0 if usr['role'] != 'admin' else 1),
                                key=f"rol_{usr['id']}",
                                format_func=lambda r: "🛡️ Administrador" if r == "admin" else "👤 Usuario",
                            )
                            if st.button("🔄 Aplicar rol", key=f"saverol_{usr['id']}", use_container_width=True):
                                if es_self and nuevo_rol != 'admin':
                                    st.error("No podés quitarte el rol de admin a vos mismo.")
                                elif nuevo_rol == usr['role']:
                                    st.info("El rol no cambió.")
                                else:
                                    # Si va a dejar el sistema sin admins, bloqueá.
                                    if usr['role'] == 'admin' and nuevo_rol != 'admin':
                                        cur.execute("SELECT COUNT(*) AS n FROM usuarios WHERE role='admin'")
                                        if (cur.fetchone() or {}).get('n', 0) <= 1:
                                            st.error("No podés degradar al único administrador.")
                                        else:
                                            try:
                                                cur.execute("UPDATE usuarios SET role=%s WHERE id=%s",
                                                            (nuevo_rol, usr['id']))
                                                conn.commit()
                                                st.success("Rol actualizado.")
                                                st.rerun()
                                            except Exception as e:
                                                conn.rollback()
                                                st.error(f"Error: {e}")
                                    else:
                                        try:
                                            cur.execute("UPDATE usuarios SET role=%s WHERE id=%s",
                                                        (nuevo_rol, usr['id']))
                                            conn.commit()
                                            st.success("Rol actualizado.")
                                            st.rerun()
                                        except Exception as e:
                                            conn.rollback()
                                            st.error(f"Error: {e}")

                        # --- Resetear contraseña ---
                        with cb:
                            with st.form(f"reset_pwd_{usr['id']}", clear_on_submit=True):
                                new_pwd = st.text_input(
                                    "Nueva contraseña", type="password",
                                    key=f"np_{usr['id']}"
                                )
                                if st.form_submit_button("🔑 Resetear", use_container_width=True):
                                    if not new_pwd or len(new_pwd) < 6:
                                        st.warning("Mínimo 6 caracteres.")
                                    else:
                                        try:
                                            hashed = bcrypt.hashpw(new_pwd.encode(), bcrypt.gensalt()).decode()
                                            cur.execute("UPDATE usuarios SET password=%s WHERE id=%s",
                                                        (hashed, usr['id']))
                                            conn.commit()
                                            st.success("Contraseña actualizada.")
                                        except Exception as e:
                                            conn.rollback()
                                            st.error(f"Error: {e}")

                        # --- Eliminar ---
                        with cc:
                            st.write(""); st.write("")
                            if es_self:
                                st.caption("No podés eliminarte a vos mismo.")
                            else:
                                confirmar = st.checkbox(
                                    f"Confirmar eliminar a {usr['username']}",
                                    key=f"delchk_{usr['id']}"
                                )
                                if st.button("🗑️ Eliminar", key=f"del_{usr['id']}", use_container_width=True):
                                    if not confirmar:
                                        st.warning("Marcá la casilla de confirmación primero.")
                                    else:
                                        # No permitir dejar sistema sin admins
                                        if usr['role'] == 'admin':
                                            cur.execute("SELECT COUNT(*) AS n FROM usuarios WHERE role='admin'")
                                            if (cur.fetchone() or {}).get('n', 0) <= 1:
                                                st.error("No podés eliminar al único administrador.")
                                                st.stop()
                                        try:
                                            # Chequear si tiene datos asociados
                                            cur.execute("""
                                                SELECT
                                                  (SELECT COUNT(*) FROM movimientos WHERE user_id=%(id)s) AS m,
                                                  (SELECT COUNT(*) FROM inversiones WHERE user_id=%(id)s) AS i,
                                                  (SELECT COUNT(*) FROM ahorros WHERE user_id=%(id)s) AS a,
                                                  (SELECT COUNT(*) FROM prestamos
                                                    WHERE prestamista_id=%(id)s OR prestatario_id=%(id)s) AS p
                                            """, {"id": usr['id']})
                                            deps = cur.fetchone() or {}
                                            total_deps = sum([deps.get(k, 0) or 0 for k in ('m','i','a','p')])
                                            if total_deps > 0:
                                                st.error(
                                                    f"El usuario tiene {total_deps} registros asociados "
                                                    f"(movimientos/préstamos/etc). Eliminá primero sus datos."
                                                )
                                            else:
                                                cur.execute("DELETE FROM usuarios WHERE id=%s", (usr['id'],))
                                                conn.commit()
                                                st.success(f"Usuario '{usr['username']}' eliminado.")
                                                st.rerun()
                                        except Exception as e:
                                            conn.rollback()
                                            st.error(f"Error: {e}")
            finally:
                cur.close()


