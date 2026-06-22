"""Helpers de formatação e logging compartilhados."""
import logging

# --- LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("app_financeiro")

def formatar_moeda(valor):
    """Formata um número no padrão monetário europeu (1.234,56), sem o símbolo €."""
    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def _para_float_br(valor):
    """Converte um valor possivelmente em string no formato europeu (1.234,56) para float."""
    if isinstance(valor, str):
        try:
            return float(valor.replace(".", "").replace(",", "."))
        except ValueError:
            return 0.0
    return valor

def cor_saldo(valor):
    """Retorna o estilo CSS de cor (verde/vermelho/preto) conforme o sinal do valor."""
    valor = _para_float_br(valor)
    cor = "red" if valor < 0 else "green" if valor > 0 else "black"
    return f"color: {cor}"
