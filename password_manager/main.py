import tkinter as tk
from tkinter import ttk, messagebox
import os
import sqlite3
import random
import string
import qrcode
import pyotp
import base64
import json
import pyperclip
import re
from datetime import datetime, timedelta
from ttkthemes import ThemedTk
from cryptography.fernet import Fernet
from PIL import Image, ImageTk
import hashlib
import binascii

# --- 1. Configurar o Arquivo de Segredo e Banco de Dados ---
MASTER_FILE = "master_hash.key"
DB_FILE = "passwords.db"
editing_id = None
F = None  # Chave de criptografia será definida após o login
# Variáveis para o bloqueio automático
last_activity = datetime.now()
IDLE_TIMEOUT = 180 # Tempo de inatividade em segundos (3 minutos)

def setup_database():
    """Conecta ao banco de dados e cria a tabela se ela não existir e adiciona as colunas de data e validade."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS passwords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                usuario TEXT NOT NULL,
                senha TEXT NOT NULL,
                last_changed TEXT,
                expiry_days INTEGER
            )""")
    c.execute("""CREATE TABLE IF NOT EXISTS password_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                password_id INTEGER NOT NULL,
                old_senha TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                FOREIGN KEY (password_id) REFERENCES passwords(id) ON DELETE CASCADE
            )""")
            
    # Adicionar as colunas se elas não existirem
    try:
        c.execute("ALTER TABLE passwords ADD COLUMN last_changed TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("ALTER TABLE passwords ADD COLUMN expiry_days INTEGER")
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("ALTER TABLE passwords ADD COLUMN observacoes TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("ALTER TABLE passwords ADD COLUMN category TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("ALTER TABLE passwords ADD COLUMN totp_secret TEXT")
    except sqlite3.OperationalError:
        pass
    
    conn.commit()
    conn.close()

def derive_key(master_password, salt):
    """Deriva a chave Fernet da senha mestra usando PBKDF2."""
    key = hashlib.pbkdf2_hmac('sha256', master_password.encode(), salt, 100000)
    return Fernet(base64.urlsafe_b64encode(key))

def on_closing():
    """Limpa a chave de criptografia da memória e fecha o aplicativo."""
    global F
    F = None
    root.destroy()

# --- Funções para Bloqueio Automático ---
def reset_timer(event=None):
    """Reinicia o temporizador de inatividade."""
    global last_activity
    last_activity = datetime.now()

def check_for_inactivity():
    """Verifica se o tempo limite de inatividade foi atingido e bloqueia o app."""
    global last_activity
    if F and datetime.now() - last_activity > timedelta(seconds=IDLE_TIMEOUT):
        logout_app()
    root.after(1000, check_for_inactivity) # Verifica a cada segundo

# --- Funções de Manipulação de Dados ---
def salvar_dados():
    global editing_id
    url = url_entry.get()
    usuario = usuario_entry.get()
    senha = senha_entry.get()
    observacoes = obs_text.get("1.0", tk.END).strip()
    category = category_entry.get()
    totp_secret = totp_entry.get()
    
    expiry_days = get_expiry_days()

    if not url or not usuario or not senha:
        messagebox.showerror("Erro", "Todos os campos devem ser preenchidos!")
        return
    
    if expiry_days is None:
        messagebox.showerror("Erro", "A validade da senha é inválida.")
        return
        
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute("SELECT id, url, usuario, senha FROM passwords")
    rows = c.fetchall()
    
    decrypted_passwords = {}
    for row in rows:
        decrypted_passwords[row[0]] = {
            "url": F.decrypt(row[1].encode()).decode(),
            "usuario": F.decrypt(row[2].encode()).decode(),
            "senha": F.decrypt(row[3].encode()).decode()
        }

    for item_id, item_data in decrypted_passwords.items():
        if item_id != editing_id and item_data['senha'] == senha:
            resposta = messagebox.askyesno("Aviso de Segurança", f"Esta senha já está a ser usada para o serviço: {item_data['url']} com o usuário {item_data['usuario']}. Deseja continuar a usar a mesma senha?")
            if not resposta:
                conn.close()
                return

    if editing_id is not None:
        c.execute("SELECT old_senha FROM password_history WHERE password_id = ?", (editing_id,))
        history_rows = c.fetchall()
        for history_row in history_rows:
            old_senha = F.decrypt(history_row[0].encode()).decode()
            if old_senha == senha:
                resposta = messagebox.askyesno("Aviso de Segurança", "Esta senha está no histórico de senhas para este item. Reutilizar senhas não é recomendado. Deseja continuar?")
                if not resposta:
                    conn.close()
                    return
    
    encrypted_senha = F.encrypt(senha.encode()).decode()
    encrypted_observacoes = F.encrypt(observacoes.encode()).decode() if observacoes else ""
    encrypted_category = F.encrypt(category.encode()).decode() if category else ""
    encrypted_totp = F.encrypt(totp_secret.encode()).decode() if totp_secret else None
    current_time = datetime.now().isoformat()

    if editing_id is not None:
        c.execute("SELECT senha FROM passwords WHERE id = ?", (editing_id,))
        old_encrypted_senha = c.fetchone()[0]
        c.execute("INSERT INTO password_history (password_id, old_senha, timestamp) VALUES (?, ?, ?)", 
                  (editing_id, old_encrypted_senha, current_time))
        
        encrypted_url = F.encrypt(url.encode()).decode()
        encrypted_usuario = F.encrypt(usuario.encode()).decode()
        c.execute("""
            UPDATE passwords SET url = ?, usuario = ?, senha = ?, last_changed = ?, expiry_days = ?, observacoes = ?, category = ?, totp_secret = ? WHERE id = ?
        """, (encrypted_url, encrypted_usuario, encrypted_senha, current_time, expiry_days, encrypted_observacoes, encrypted_category, encrypted_totp, editing_id))
        editing_id = None
    else:
        encrypted_url = F.encrypt(url.encode()).decode()
        encrypted_usuario = F.encrypt(usuario.encode()).decode()
        c.execute("INSERT INTO passwords (url, usuario, senha, last_changed, expiry_days, observacoes, category, totp_secret) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
                  (encrypted_url, encrypted_usuario, encrypted_senha, current_time, expiry_days, encrypted_observacoes, encrypted_category, encrypted_totp))
    
    conn.commit()
    conn.close()

    messagebox.showinfo("Sucesso!", "Dados salvos/atualizados com sucesso!")
    
    url_entry.delete(0, tk.END)
    usuario_entry.delete(0, tk.END)
    senha_entry.delete(0, tk.END)
    obs_text.delete("1.0", tk.END)
    category_entry.set("")
    totp_entry.delete(0, tk.END)
    
    populate_treeview()
    reset_timer()

def excluir_dados():
    selected_item = tree.selection()
    if not selected_item:
        messagebox.showerror("Erro", "Nenhum item selecionado para exclusão!")
        return

    item_id = tree.item(selected_item[0])['values'][0]
    
    resposta = messagebox.askyesno("Confirmar", "Tem certeza que deseja excluir esta senha? O histórico também será excluído.")
    if resposta:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("DELETE FROM passwords WHERE id = ?", (item_id,))
        conn.commit()
        conn.close()
        
        populate_treeview()
        messagebox.showinfo("Sucesso!", "Senha excluída com sucesso!")
        reset_timer()

def carregar_para_edicao(event):
    global editing_id
    selected_item = tree.selection()
    if not selected_item:
        return
    
    item_id = tree.item(selected_item[0])['values'][0]
    editing_id = item_id

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT url, usuario, senha, expiry_days, observacoes, category, totp_secret FROM passwords WHERE id = ?", (item_id,))
    row = c.fetchone()
    conn.close()

    if row:
        decrypted_url = F.decrypt(row[0].encode()).decode()
        decrypted_usuario = F.decrypt(row[1].encode()).decode()
        decrypted_senha = F.decrypt(row[2].encode()).decode()
        expiry_days = row[3]
        decrypted_obs = F.decrypt(row[4].encode()).decode() if row[4] else ""
        decrypted_category = F.decrypt(row[5].encode()).decode() if row[5] else ""
        decrypted_totp = F.decrypt(row[6].encode()).decode() if row[6] else ""

        url_entry.delete(0, tk.END)
        url_entry.insert(0, decrypted_url)
        
        usuario_entry.delete(0, tk.END)
        usuario_entry.insert(0, decrypted_usuario)
        
        senha_entry.config(show='')
        senha_entry.delete(0, tk.END)
        senha_entry.insert(0, decrypted_senha)
        toggle_password_visibility(force_hide=True)

        obs_text.delete("1.0", tk.END)
        obs_text.insert("1.0", decrypted_obs)

        set_expiry_options(expiry_days)
        
        category_entry.set(decrypted_category)
        
        totp_entry.delete(0, tk.END)
        totp_entry.insert(0, decrypted_totp)

    reset_timer()

def set_expiry_options(days):
    if days == -1:
        expiry_var.set("Sem validade")
        custom_entry.config(state="disabled")
    elif days in [30, 45, 60, 90]:
        expiry_var.set(str(days))
        custom_entry.config(state="disabled")
    else:
        expiry_var.set("Personalizado")
        custom_entry.config(state="normal")
        custom_entry.delete(0, tk.END)
        custom_entry.insert(0, str(days))

def get_expiry_days():
    selected_option = expiry_var.get()
    if selected_option == "Sem validade":
        return -1
    elif selected_option == "Personalizado":
        try:
            days = int(custom_entry.get())
            if days <= 0:
                return None
            return days
        except ValueError:
            return None
    else:
        return int(selected_option)

def on_radio_select():
    if expiry_var.get() == "Personalizado":
        custom_entry.config(state="normal")
    else:
        custom_entry.config(state="disabled")
    reset_timer()

def buscar_dados():
    termo = busca_entry.get()
    populate_treeview(termo)
    reset_timer()

def populate_treeview(search_term=""):
    tree.delete(*tree.get_children())
    
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id, url, usuario, last_changed, expiry_days, category, totp_secret FROM passwords")
    rows = c.fetchall()
    conn.close()

    tree.tag_configure("fraca", background="#e85a5a", foreground="white")
    tree.tag_configure("expirada", background="#f5d37d", foreground="#333333")
    tree.tag_configure("normal", background="#E0E0E0", foreground="#333333")

    if rows:
        for row in rows:
            decrypted_url = F.decrypt(row[1].encode()).decode()
            decrypted_usuario = F.decrypt(row[2].encode()).decode()
            decrypted_category = F.decrypt(row[5].encode()).decode() if row[5] else ""
            
            last_changed_iso = row[3] if row[3] is not None else "N/A"
            last_changed_formatted = "N/A"
            if last_changed_iso != "N/A":
                try:
                    dt_obj = datetime.fromisoformat(last_changed_iso)
                    last_changed_formatted = dt_obj.strftime("%d/%m/%Y %H:%M")
                except ValueError:
                    pass
            
            expiry_days = row[4] if row[4] is not None else -1
            
            if search_term.lower() in decrypted_url.lower() or search_term.lower() in decrypted_usuario.lower() or search_term.lower() in decrypted_category.lower():
                tags = ()
                if expiry_days != -1 and last_changed_iso != "N/A":
                    try:
                        last_changed_date = datetime.fromisoformat(last_changed_iso)
                        if datetime.now() > last_changed_date + timedelta(days=expiry_days):
                            tags = ("expirada",)
                    except ValueError:
                        pass
                
                validade_str = "Sem validade" if expiry_days == -1 else f"{expiry_days} dias"
                tree.insert("", "end", iid=row[0], values=(row[0], decrypted_url, decrypted_usuario, last_changed_formatted, validade_str, decrypted_category), tags=tags)


def get_all_categories():
    """Obtém uma lista de todas as categorias únicas do banco de dados."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT DISTINCT category FROM passwords WHERE category IS NOT NULL AND category != ''")
    rows = c.fetchall()
    conn.close()
    
    categories = [F.decrypt(row[0].encode()).decode() for row in rows]
    return sorted(list(set(categories)))

def check_for_weak_passwords():
    """Verifica todas as senhas salvas e destaca as fracas e expiradas."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id, senha, last_changed, expiry_days FROM passwords")
    rows = c.fetchall()
    conn.close()
    
    for item in tree.get_children():
        tree.item(item, tags=())

    weak_count = 0
    expired_count = 0

    if rows:
        for row in rows:
            decrypted_senha = F.decrypt(row[1].encode()).decode()
            score = check_password_strength(decrypted_senha)
            tags = []
            
            if score < 50:
                tags.append("fraca")
                weak_count += 1
            
            last_changed = row[2]
            expiry_days = row[3]
            
            if expiry_days is not None and expiry_days != -1 and last_changed is not None:
                try:
                    last_changed_date = datetime.fromisoformat(last_changed)
                    if datetime.now() > last_changed_date + timedelta(days=expiry_days):
                        tags.append("expirada")
                        expired_count += 1
                except ValueError:
                    pass
            
            if tree.exists(row[0]):
                tree.item(row[0], tags=tags)
    
    if weak_count > 0 or expired_count > 0:
        message = ""
        if weak_count > 0:
            message += f"Encontradas {weak_count} senhas fracas. Elas foram destacadas em vermelho.\n"
        if expired_count > 0:
            message += f"Encontradas {expired_count} senhas expiradas. Elas foram destacadas em amarelo."
        messagebox.showwarning("Avisos de Segurança", message)
    else:
        messagebox.showinfo("Nenhuma Senha Fraca ou Expirada", "Todas as suas senhas parecem ser fortes e estão dentro do prazo de validade!")
    reset_timer()

# --- Funções do Gerador de Senhas ---
def check_password_strength(password):
    score = 0
    if len(password) >= 8:
        score += 25
    if len(password) >= 12:
        score += 25
    if re.search(r"[a-z]", password):
        score += 10
    if re.search(r"[A-Z]", password):
        score += 10
    if re.search(r"\d", password):
        score += 15
    if re.search(r"[!@#$%^&*()_+-=\[\]{};':\",.<>/?`~|\\ ]", password):
        score += 15
    
    return min(score, 100)

def gerar_senha(length, chars_maiusculas, chars_minusculas, chars_digitos, chars_simbolos, resultado_entry, strength_bar):
    character_set = ""
    if chars_maiusculas.get():
        character_set += string.ascii_uppercase
    if chars_minusculas.get():
        character_set += string.ascii_lowercase
    if chars_digitos.get():
        character_set += string.digits
    if chars_simbolos.get():
        character_set += string.punctuation
    
    if not character_set:
        messagebox.showerror("Erro", "Selecione ao menos um tipo de caractere.")
        return ""
        
    password = ''.join(random.choice(character_set) for _ in range(length))
    resultado_entry.delete(0, tk.END)
    resultado_entry.insert(0, password)
    update_strength_bar(password, strength_bar)
    reset_timer()

def update_strength_bar(password, strength_bar):
    score = check_password_strength(password)
    strength_bar['value'] = score
    if score < 50:
        strength_bar.configure(style="red.Horizontal.TProgressbar")
    elif score < 75:
        strength_bar.configure(style="yellow.Horizontal.TProgressbar")
    else:
        strength_bar.configure(style="green.Horizontal.TProgressbar")
    strength_bar.update()

def abrir_gerador_senha():
    gerador_window = tk.Toplevel(root)
    gerador_window.title("Gerador de Senhas")
    gerador_window.geometry("450x300")
    gerador_window.bind("<Key>", reset_timer)
    gerador_window.bind("<Button-1>", reset_timer)

    main_frame_gerador = ttk.Frame(gerador_window, padding=20)
    main_frame_gerador.pack(expand=True, fill='both')

    length_label = ttk.Label(main_frame_gerador, text="Comprimento da Senha:")
    length_label.pack(pady=(0, 5))
    
    length_var = tk.IntVar(value=12)
    length_value_label = ttk.Label(main_frame_gerador, textvariable=length_var)
    length_value_label.pack()
    
    def atualizar_comprimento(val):
        length_var.set(int(float(val)))
        
    length_scale = ttk.Scale(main_frame_gerador, from_=4, to=50, orient='horizontal', variable=length_var, command=atualizar_comprimento)
    length_scale.pack(fill='x')

    chars_maiusculas = tk.BooleanVar(value=True)
    chars_minusculas = tk.BooleanVar(value=True)
    chars_digitos = tk.BooleanVar(value=True)
    chars_simbolos = tk.BooleanVar(value=True)

    ttk.Checkbutton(main_frame_gerador, text="Letras Maiúsculas (ABC)", variable=chars_maiusculas).pack(anchor='w')
    ttk.Checkbutton(main_frame_gerador, text="Letras Minúsculas (abc)", variable=chars_minusculas).pack(anchor='w')
    ttk.Checkbutton(main_frame_gerador, text="Dígitos (123)", variable=chars_digitos).pack(anchor='w')
    ttk.Checkbutton(main_frame_gerador, text="Símbolos (!@#)", variable=chars_simbolos).pack(anchor='w')

    resultado_entry = ttk.Entry(main_frame_gerador, width=40)
    resultado_entry.pack(pady=(10, 5))
    
    strength_bar = ttk.Progressbar(main_frame_gerador, orient='horizontal', length=200, mode='determinate')
    strength_bar.pack(pady=(0, 5))
    style = ttk.Style()
    style.configure("red.Horizontal.TProgressbar", background='red')
    style.configure("yellow.Horizontal.TProgressbar", background='yellow')
    style.configure("green.Horizontal.TProgressbar", background='green')

    def on_gerar():
        gerar_senha(length_var.get(), chars_maiusculas, chars_minusculas, chars_digitos, chars_simbolos, resultado_entry, strength_bar)
    
    gerar_button = ttk.Button(main_frame_gerador, text="Gerar Senha", command=on_gerar)
    gerar_button.pack(fill='x', pady=(5, 5))

    def copiar_para_principal():
        senha = resultado_entry.get()
        if senha:
            senha_entry.delete(0, tk.END)
            senha_entry.insert(0, senha)
            gerador_window.destroy()
            reset_timer()

    copiar_button = ttk.Button(main_frame_gerador, text="Usar esta Senha", command=copiar_para_principal)
    copiar_button.pack(fill='x')
    
    update_strength_bar("", strength_bar)
    reset_timer()
    
def exportar_dados():
    """Exporta todas as senhas para um arquivo encriptado."""
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT url, usuario, senha, observacoes, category, totp_secret FROM passwords")
        rows = c.fetchall()
        conn.close()

        data_list = []
        for row in rows:
            decrypted_url = F.decrypt(row[0].encode()).decode()
            decrypted_usuario = F.decrypt(row[1].encode()).decode()
            decrypted_senha = F.decrypt(row[2].encode()).decode()
            decrypted_obs = F.decrypt(row[3].encode()).decode() if row[3] else ""
            decrypted_category = F.decrypt(row[4].encode()).decode() if row[4] else ""
            decrypted_totp = F.decrypt(row[5].encode()).decode() if row[5] else ""
            data_list.append({
                "url": decrypted_url,
                "usuario": decrypted_usuario,
                "senha": decrypted_senha,
                "observacoes": decrypted_obs,
                "category": decrypted_category,
                "totp_secret": decrypted_totp
            })
        
        json_data = json.dumps(data_list).encode('utf-8')
        encrypted_data = F.encrypt(json_data)
        
        with open("senhas_exportadas.enc", "wb") as f:
            f.write(encrypted_data)
        
        messagebox.showinfo("Sucesso", "Dados exportados para 'senhas_exportadas.enc' com sucesso!")
    except Exception as e:
        messagebox.showerror("Erro de Exportação", f"Ocorreu um erro: {e}")
    reset_timer()

def importar_dados():
    """Importa senhas de um arquivo encriptado e salva no banco de dados."""
    if not os.path.exists("senhas_exportadas.enc"):
        messagebox.showerror("Erro de Importação", "Arquivo 'senhas_exportadas.enc' não encontrado.")
        return
        
    resposta = messagebox.askyesno("Confirmar Importação", "Isto irá adicionar os dados do arquivo ao seu banco de dados atual. Tem certeza que deseja continuar?")
    if not resposta:
        return
        
    try:
        with open("senhas_exportadas.enc", "rb") as f:
            encrypted_data = f.read()
        
        decrypted_data = F.decrypt(encrypted_data).decode('utf-8')
        data_list = json.loads(decrypted_data)
        
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        for item in data_list:
            encrypted_url = F.encrypt(item["url"].encode()).decode()
            encrypted_usuario = F.encrypt(item["usuario"].encode()).decode()
            encrypted_senha = F.encrypt(item["senha"].encode()).decode()
            encrypted_obs = F.encrypt(item.get("observacoes", "").encode()).decode()
            encrypted_category = F.encrypt(item.get("category", "").encode()).decode()
            encrypted_totp = F.encrypt(item.get("totp_secret", "").encode()).decode() if item.get("totp_secret") else None
            # Validade padrão para importação
            c.execute("INSERT INTO passwords (url, usuario, senha, last_changed, expiry_days, observacoes, category, totp_secret) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
                      (encrypted_url, encrypted_usuario, encrypted_senha, datetime.now().isoformat(), 180, encrypted_obs, encrypted_category, encrypted_totp))
        
        conn.commit()
        conn.close()
        populate_treeview()
        messagebox.showinfo("Sucesso", "Dados importados com sucesso!")
        
    except Exception as e:
        messagebox.showerror("Erro de Importação", f"Ocorreu um erro ao importar os dados: {e}")
    reset_timer()

def copiar_senha():
    selected_item = tree.selection()
    if not selected_item:
        messagebox.showerror("Erro", "Nenhum item selecionado.")
        return
    
    item_id = tree.item(selected_item[0])['values'][0]
    
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT senha FROM passwords WHERE id = ?", (item_id,))
    row = c.fetchone()
    conn.close()

    if row:
        decrypted_senha = F.decrypt(row[0].encode()).decode()
        pyperclip.copy(decrypted_senha)
        messagebox.showinfo("Sucesso", "Senha copiada para a área de transferência!")
    reset_timer()

def abrir_historico_senha():
    selected_item = tree.selection()
    if not selected_item:
        messagebox.showerror("Erro", "Nenhum item selecionado.")
        return
    
    item_id = tree.item(selected_item[0])['values'][0]

    historico_win = tk.Toplevel(root)
    historico_win.title("Histórico de Senhas")
    historico_win.geometry("500x300")
    historico_win.bind("<Key>", reset_timer)
    historico_win.bind("<Button-1>", reset_timer)

    historico_label = ttk.Label(historico_win, text="Senhas anteriores para este item:", font=("Helvetica", 12))
    historico_label.pack(pady=10)

    historico_tree = ttk.Treeview(historico_win, columns=("Senha", "Data da Alteração"), show="headings")
    historico_tree.heading("Senha", text="Senha")
    historico_tree.heading("Data da Alteração", text="Data da Alteração")
    historico_tree.column("Senha", width=200)
    historico_tree.column("Data da Alteração", width=200)
    historico_tree.pack(expand=True, fill='both', padx=10, pady=5)

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT old_senha, timestamp FROM password_history WHERE password_id = ? ORDER BY timestamp DESC", (item_id,))
    history_rows = c.fetchall()
    conn.close()

    for row in history_rows:
        decrypted_senha = F.decrypt(row[0].encode()).decode()
        
        timestamp_iso = row[1]
        timestamp_formatted = "N/A"
        if timestamp_iso != "N/A":
            try:
                dt_obj = datetime.fromisoformat(timestamp_iso)
                timestamp_formatted = dt_obj.strftime("%d/%m/%Y %H:%M")
            except ValueError:
                pass
        
        historico_tree.insert("", "end", values=(decrypted_senha, timestamp_formatted))
        
    reset_timer()
        
def toggle_password_visibility(force_hide=False):
    if force_hide:
        senha_entry.config(show='*')
    else:
        if senha_entry.cget('show') == '*':
            senha_entry.config(show='')
        else:
            senha_entry.config(show='*')
    reset_timer()

# --- Funções para TOTP (dentro do app) ---
def gerar_totp_code():
    selected_item = tree.selection()
    if not selected_item:
        messagebox.showerror("Erro", "Nenhum item selecionado.")
        return

    item_id = tree.item(selected_item[0])['values'][0]

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT totp_secret FROM passwords WHERE id = ?", (item_id,))
    row = c.fetchone()
    conn.close()

    if row and row[0]:
        try:
            decrypted_secret = F.decrypt(row[0].encode()).decode()
            totp = pyotp.TOTP(decrypted_secret)
            code = totp.now()
            pyperclip.copy(code)
            messagebox.showinfo("Código TOTP Gerado", f"O código TOTP atual é: {code}\n(Copiado para a área de transferência)")
        except Exception as e:
            messagebox.showerror("Erro TOTP", f"Ocorreu um erro ao gerar o código TOTP: {e}")
    else:
        messagebox.showwarning("Aviso", "Este item não tem uma chave TOTP associada.")
    reset_timer()

def gerar_qr_code():
    """Gera um QR Code para a chave TOTP de uma senha e o exibe."""
    if not totp_entry.get():
        messagebox.showwarning("Aviso", "O campo TOTP está vazio.")
        return
        
    try:
        decrypted_secret = totp_entry.get()
        # A URL precisa de um nome de utilizador para ser válida
        usuario = usuario_entry.get() if usuario_entry.get() else "totp_key"
        url = f"otpauth://totp/LocalPasswordManager:{usuario}?secret={decrypted_secret}&issuer=LocalPasswordManager"

        qr_win = tk.Toplevel(root)
        qr_win.title("Código QR TOTP")
        qr_win.geometry("300x300")
        qr_win.resizable(False, False)
        qr_win.grab_set()
        
        img = qrcode.make(url, box_size=5)
        
        temp_file = "temp_qr.png"
        img.save(temp_file)
        
        photo = tk.PhotoImage(file=temp_file)
        qr_label = ttk.Label(qr_win, image=photo)
        qr_label.image = photo
        qr_label.pack(padx=10, pady=10)
        
        os.remove(temp_file)
        
    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro ao gerar o QR Code: {e}")
    reset_timer()

def iniciar_aplicacao():
    global root, url_entry, usuario_entry, senha_entry, obs_text, category_entry, totp_entry, tree, senha_entry_show, busca_entry, expiry_var, custom_entry, main_frame
    
    for widget in root.winfo_children():
        widget.destroy()

    root.deiconify()
    
    root.bind_all("<Button-1>", reset_timer)
    root.bind_all("<Key>", reset_timer)
    
    style = ttk.Style()
    style.configure("TLabel", font=("Helvetica", 10))
    style.configure("TButton", font=("Helvetica", 10, "bold"))
    style.configure("Treeview.Heading", font=('Helvetica', 10, 'bold'))
    style.configure("Treeview", rowheight=25)
    style.configure("TEntry", fieldbackground="#F0F0F0")
    style.configure("TFrame", background="#E0E0E0")
    style.map("Treeview", background=[('selected', 'blue')], foreground=[('selected', 'white')])
    style.configure("Treeview", background="#FFFFFF", foreground="#000000", fieldbackground="#FFFFFF")
    style.map("Treeview", background=[('selected', '#3399FF')])
    style.map("Treeview", foreground=[('selected', 'white')])

    main_frame = ttk.Frame(root, padding=20)
    main_frame.pack(expand=True, fill='both')

    title_label = ttk.Label(main_frame, text="Local Password Manager", font=("Helvetica", 16))
    title_label.pack(pady=(0, 15))

    url_frame = ttk.Frame(main_frame)
    url_frame.pack(fill='x', padx=0, pady=(5, 0))
    url_label = ttk.Label(url_frame, text="URL/Serviço:")
    url_label.pack(anchor='w')
    url_entry = ttk.Entry(url_frame, width=40)
    url_entry.pack(fill='x', expand=True)

    usuario_frame = ttk.Frame(main_frame)
    usuario_frame.pack(fill='x', padx=0, pady=(5, 0))
    usuario_label = ttk.Label(usuario_frame, text="Usuário:")
    usuario_label.pack(anchor='w')
    usuario_entry = ttk.Entry(usuario_frame, width=40)
    usuario_entry.pack(fill='x', expand=True)

    senha_frame = ttk.Frame(main_frame)
    senha_frame.pack(fill='x', padx=0, pady=(5, 0))
    senha_label = ttk.Label(senha_frame, text="Senha:")
    senha_label.pack(anchor='w')
    senha_entry_container = ttk.Frame(senha_frame)
    senha_entry_container.pack(fill='x', expand=True)
    senha_entry_show = tk.StringVar()
    senha_entry = ttk.Entry(senha_entry_container, show='*', textvariable=senha_entry_show)
    senha_entry.pack(side=tk.LEFT, fill='x', expand=True)

    try:
        eye_icon = Image.open("eye_icon.png").resize((20, 20))
        eye_icon_tk = ImageTk.PhotoImage(eye_icon)
        senha_entry_container.eye_icon = eye_icon_tk
        eye_button = ttk.Button(senha_entry_container, image=eye_icon_tk, command=toggle_password_visibility)
        eye_button.pack(side=tk.LEFT, padx=(5,0))
    except FileNotFoundError:
        messagebox.showwarning("Aviso", "Arquivo 'eye_icon.png' não encontrado. O botão de visibilidade não funcionará.")
        
    obs_frame = ttk.Frame(main_frame)
    obs_frame.pack(fill='x', padx=0, pady=(5, 0))
    obs_label = ttk.Label(obs_frame, text="Observações:")
    obs_label.pack(anchor='w')
    obs_text = tk.Text(obs_frame, height=4, width=40)
    obs_text.pack(fill='x', expand=True)

    category_frame = ttk.Frame(main_frame)
    category_frame.pack(fill='x', padx=0, pady=(5,0))
    category_label = ttk.Label(category_frame, text="Categoria:")
    category_label.pack(anchor='w')
    
    categories = get_all_categories()
    category_entry = ttk.Combobox(category_frame, values=categories)
    category_entry.pack(fill='x', expand=True)

    totp_frame = ttk.Frame(main_frame)
    totp_frame.pack(fill='x', padx=0, pady=(5,0))
    totp_label = ttk.Label(totp_frame, text="TOTP (Chave Secreta):")
    totp_label.pack(anchor='w')
    totp_entry = ttk.Entry(totp_frame, width=40)
    totp_entry.pack(fill='x', expand=True)
    
    qr_button = ttk.Button(totp_frame, text="Gerar QR Code", command=gerar_qr_code)
    qr_button.pack(pady=(5,0))

    validade_frame = ttk.Frame(main_frame)
    validade_frame.pack(fill='x', padx=0, pady=(15, 10))

    validade_label = ttk.Label(validade_frame, text="Validade da Senha:")
    validade_label.pack(side='left', padx=(0, 10))

    expiry_var = tk.StringVar(value="Sem validade")

    options_frame = ttk.Frame(validade_frame)
    options_frame.pack(side='left')

    radio_sem_validade = ttk.Radiobutton(options_frame, text="Sem validade", variable=expiry_var, value="Sem validade", command=on_radio_select)
    radio_sem_validade.pack(side='left', padx=(0, 5))

    radio_30 = ttk.Radiobutton(options_frame, text="30 dias", variable=expiry_var, value="30", command=on_radio_select)
    radio_30.pack(side='left', padx=(0, 5))

    radio_45 = ttk.Radiobutton(options_frame, text="45 dias", variable=expiry_var, value="45", command=on_radio_select)
    radio_45.pack(side='left', padx=(0, 5))

    radio_60 = ttk.Radiobutton(options_frame, text="60 dias", variable=expiry_var, value="60", command=on_radio_select)
    radio_60.pack(side='left', padx=(0, 5))

    radio_90 = ttk.Radiobutton(options_frame, text="90 dias", variable=expiry_var, value="90", command=on_radio_select)
    radio_90.pack(side='left', padx=(0, 5))

    radio_custom = ttk.Radiobutton(options_frame, text="Personalizado:", variable=expiry_var, value="Personalizado", command=on_radio_select)
    radio_custom.pack(side='left', padx=(0, 5))

    custom_entry = ttk.Entry(options_frame, width=5, state="disabled")
    custom_entry.pack(side='left')
    
    button_frame = ttk.Frame(main_frame)
    button_frame.pack(fill='x', pady=(10, 5))

    salvar_button = ttk.Button(button_frame, text="Salvar", command=salvar_dados)
    salvar_button.pack(side='left', expand=True, fill='x', padx=(0, 5))

    excluir_button = ttk.Button(button_frame, text="Excluir", command=excluir_dados)
    excluir_button.pack(side='left', expand=True, fill='x', padx=(5, 0))

    gerador_button = ttk.Button(button_frame, text="Gerar Senha", command=abrir_gerador_senha)
    gerador_button.pack(side='left', expand=True, fill='x', padx=(5, 0))

    action_frame = ttk.Frame(main_frame)
    action_frame.pack(fill='x', pady=(5, 15))
    
    exportar_button = ttk.Button(action_frame, text="Exportar Dados", command=exportar_dados)
    exportar_button.pack(side='left', expand=True, fill='x', padx=(0, 5))
    
    importar_button = ttk.Button(action_frame, text="Importar Dados", command=importar_dados)
    importar_button.pack(side='left', expand=True, fill='x', padx=(5, 0))

    copiar_button = ttk.Button(action_frame, text="Copiar Senha", command=copiar_senha)
    copiar_button.pack(side='left', expand=True, fill='x', padx=(5, 0))
    
    # === Botão "Gerar TOTP" removido ===
    
    history_button = ttk.Button(action_frame, text="Ver Histórico", command=abrir_historico_senha)
    history_button.pack(side='left', expand=True, fill='x', padx=(5, 0))

    check_button = ttk.Button(action_frame, text="Verificar Senhas", command=check_for_weak_passwords)
    check_button.pack(side='left', expand=True, fill='x', padx=(5, 0))

    logout_button = ttk.Button(action_frame, text="Sair e Bloquear", command=logout_app)
    logout_button.pack(side='left', expand=True, fill='x', padx=(5, 0))
    
    busca_frame = ttk.Frame(main_frame)
    busca_frame.pack(fill='x', pady=(15, 5))

    busca_label = ttk.Label(busca_frame, text="Buscar:")
    busca_label.pack(side='left', padx=(0, 5))

    busca_entry = ttk.Entry(busca_frame, width=30)
    busca_entry.pack(side='left', expand=True, fill='x')

    buscar_button = ttk.Button(busca_frame, text="Buscar", command=buscar_dados)
    buscar_button.pack(side='left', padx=(5, 0))

    tree = ttk.Treeview(main_frame, columns=("id", "URL", "Usuário", "Última Alteração", "Validade", "Categoria"), show="headings")
    tree.heading("URL", text="URL/Serviço")
    tree.heading("Usuário", text="Usuário")
    tree.heading("Última Alteração", text="Última Alteração")
    tree.heading("Validade", text="Validade")
    tree.heading("Categoria", text="Categoria")

    tree.column("id", width=0, stretch=tk.NO)
    tree.column("URL", width=160, anchor=tk.W)
    tree.column("Usuário", width=120, anchor=tk.W)
    tree.column("Última Alteração", width=120, anchor=tk.W)
    tree.column("Validade", width=100, anchor=tk.W)
    tree.column("Categoria", width=100, anchor=tk.W)

    tree.pack(expand=True, fill='both', pady=10)

    tree.bind("<Double-1>", carregar_para_edicao)

    populate_treeview()
    
def logout_app():
    """Limpa a chave da memória, esconde a janela principal e retorna para a tela de login."""
    global F
    F = None
    root.withdraw()
    login_window()
    
def recovery_key_window():
    """Exibe a janela da chave de recuperação."""
    rec_win = tk.Toplevel(root)
    rec_win.title("Chave de Recuperação")
    rec_win.geometry("450x250")
    rec_win.resizable(False, False)
    rec_win.grab_set()
    rec_win.protocol("WM_DELETE_WINDOW", lambda: root.destroy())
    rec_win.bind("<Key>", reset_timer)
    rec_win.bind("<Button-1>", reset_timer)

    main_frame = ttk.Frame(rec_win, padding=20)
    main_frame.pack(expand=True, fill='both')

    warning_label = ttk.Label(main_frame, text="ATENÇÃO: ESTE É O SEU CÓDIGO DE RECUPERAÇÃO!", font=("Helvetica", 12, "bold"), foreground="red")
    warning_label.pack(pady=(0, 10))

    info_label = ttk.Label(main_frame, text="Guarde este código em um lugar seguro. Ele será necessário para recuperar o acesso caso você esqueça a sua senha mestra. Ele NÃO será mostrado novamente.", wraplength=400)
    info_label.pack(pady=(0, 10))

    key_label = ttk.Label(main_frame, text="A chave de recuperação foi guardada no arquivo 'master_hash.key'", font=("Courier", 12), relief="solid", borderwidth=1, padding=5)
    key_label.pack(pady=(5, 10), fill='x')

    def copy_key():
        messagebox.showinfo("Copiado", "A chave de recuperação não pode ser copiada.")
        reset_timer()

    copy_button = ttk.Button(main_frame, text="Copiar para a Área de Transferência", command=copy_key)
    copy_button.pack(pady=(0, 10))
    
    def on_ok():
        rec_win.destroy()
        login_window()
        
    ok_button = ttk.Button(main_frame, text="Entendi. Continuar.", command=on_ok)
    ok_button.pack(fill='x')

def setup_totp():
    """Cria e exibe a tela de configuração do TOTP, exigindo a validação."""
    secret = pyotp.random_base32()
    
    # 1. Ler o conteúdo existente do arquivo (salt e hash)
    with open(MASTER_FILE, "rb") as f:
        existing_data = f.read()

    # 2. Anexar a nova chave secreta aos dados existentes
    new_data = existing_data + base64.urlsafe_b64encode(secret.encode())

    # 3. Reescrever todo o arquivo com os dados completos
    with open(MASTER_FILE, "wb") as f:
        f.write(new_data)
        
    totp_win = tk.Toplevel(root)
    totp_win.title("Configurar 2FA (TOTP)")
    totp_win.geometry("350x450")
    totp_win.resizable(False, False)
    totp_win.grab_set()
    totp_win.protocol("WM_DELETE_WINDOW", lambda: root.destroy())

    main_frame = ttk.Frame(totp_win, padding=20)
    main_frame.pack(expand=True, fill='both')

    info_label = ttk.Label(main_frame, text="1. Escaneie este QR Code com seu app de autenticação (Google Authenticator, Authy, etc.):", wraplength=300, justify=tk.CENTER)
    info_label.pack(pady=(0, 10))

    try:
        url = pyotp.totp.TOTP(secret).provisioning_uri(name="gerenciador_mauro", issuer_name="PasswordManager")
        img = qrcode.make(url, box_size=5)
        temp_file = "temp_qr_login.png"
        img.save(temp_file)
        photo = tk.PhotoImage(file=temp_file)
        qr_label = ttk.Label(main_frame, image=photo)
        qr_label.image = photo
        qr_label.pack(pady=(0, 10))
        os.remove(temp_file)
    except Exception as e:
        messagebox.showerror("Erro QR Code", f"Erro ao gerar QR Code: {e}")
    
    info_label_2 = ttk.Label(main_frame, text="2. Insira o código TOTP para confirmar:", wraplength=300, justify=tk.CENTER)
    info_label_2.pack(pady=(10, 5))
    
    totp_entry = ttk.Entry(main_frame, width=40)
    totp_entry.pack(pady=(0, 10))
    
    def validate_and_proceed():
        totp_code = totp_entry.get()
        if not totp_code:
            messagebox.showerror("Erro", "O código não pode ser vazio.")
            return
            
        totp = pyotp.TOTP(secret)
        if totp.verify(totp_code):
            totp_win.destroy()
            iniciar_aplicacao()
        else:
            messagebox.showerror("Erro", "Código TOTP inválido. Tente novamente.")
            totp_entry.delete(0, tk.END)

    validate_button = ttk.Button(main_frame, text="Validar e Continuar", command=validate_and_proceed)
    validate_button.pack(pady=(10, 0))
    totp_entry.focus()
    totp_entry.bind("<Return>", lambda event=None: validate_and_proceed())

def create_master_password_window():
    """Janela para criar a senha mestra na primeira execução."""
    create_win = tk.Toplevel(root)
    create_win.title("Definir Senha Mestra")
    create_win.geometry("350x200")
    create_win.resizable(False, False)
    create_win.grab_set() 
    create_win.protocol("WM_DELETE_WINDOW", lambda: root.destroy())
    
    main_frame = ttk.Frame(create_win, padding=20)
    main_frame.pack(expand=True, fill='both')

    label = ttk.Label(main_frame, text="Crie uma Senha Mestra:", font=("Helvetica", 10))
    label.pack(pady=(0, 5))

    pass_entry = ttk.Entry(main_frame, width=30, show='*')
    pass_entry.pack(pady=(0, 5))

    confirm_label = ttk.Label(main_frame, text="Confirme a Senha:", font=("Helvetica", 10))
    confirm_label.pack(pady=(5, 5))

    confirm_entry = ttk.Entry(main_frame, width=30, show='*')
    confirm_entry.pack(pady=(0, 10))

    def on_create():
        global F
        password = pass_entry.get()
        confirm_password = confirm_entry.get()
        
        if not password or password != confirm_password:
            messagebox.showerror("Erro", "As senhas não coincidem ou estão vazias.")
            return

        salt = os.urandom(16)
        hashed_password = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 100000)
        
        recovery_key = base64.urlsafe_b64encode(os.urandom(32)).decode('utf-8')
        hashed_recovery_key = hashlib.sha256(recovery_key.encode()).digest()

        with open(MASTER_FILE, "wb") as f:
            f.write(salt + hashed_password + hashed_recovery_key)
        
        F = derive_key(password, salt)
        
        create_win.destroy()
        show_recovery_key_window(recovery_key) # Nova chamada de função

    create_button = ttk.Button(main_frame, text="Criar Senha", command=on_create)
    create_button.pack(fill='x')
    pass_entry.focus()
    pass_entry.bind("<Return>", lambda event=None: on_create())
    confirm_entry.bind("<Return>", lambda event=None: on_create())
    
def recover_password_window():
    """Janela para recuperação de senha."""
    rec_win = tk.Toplevel(root)
    rec_win.title("Recuperar Senha")
    rec_win.geometry("400x200")
    rec_win.resizable(False, False)
    rec_win.grab_set()
    rec_win.protocol("WM_DELETE_WINDOW", lambda: root.destroy())

    main_frame = ttk.Frame(rec_win, padding=20)
    main_frame.pack(expand=True, fill='both')

    info_label = ttk.Label(main_frame, text="Insira sua Chave de Recuperação:", font=("Helvetica", 10))
    info_label.pack(pady=(0, 5))

    key_entry = ttk.Entry(main_frame, width=40)
    key_entry.pack(pady=(0, 10))

    def on_recover():
        entered_key = key_entry.get()
        if not entered_key:
            messagebox.showerror("Erro", "O campo não pode ficar vazio.")
            return

        try:
            with open(MASTER_FILE, "rb") as f:
                data = f.read()
                stored_recovery_hash = data[16+32:]
            
            entered_hash = hashlib.sha256(entered_key.encode()).digest()

            if entered_hash == stored_recovery_hash:
                os.remove(MASTER_FILE)
                rec_win.destroy()
                messagebox.showinfo("Sucesso", "Chave de recuperação válida. Crie sua nova senha mestra.")
                create_master_password_window()
            else:
                messagebox.showerror("Erro", "Chave de recuperação incorreta.")
        except FileNotFoundError:
            messagebox.showerror("Erro", "Arquivo de segurança não encontrado. O aplicativo precisa ser reiniciado.")
    
    recover_button = ttk.Button(main_frame, text="Recuperar Senha", command=on_recover)
    recover_button.pack(fill='x')
    
    key_entry.focus()
    key_entry.bind("<Return>", lambda event=None: on_recover())

def totp_window(totp_secret):
    """Janela para inserir o código TOTP no login."""
    totp_win = tk.Toplevel(root)
    totp_win.title("Código de Autenticação")
    totp_win.geometry("350x200")
    totp_win.resizable(False, False)
    totp_win.grab_set()
    totp_win.protocol("WM_DELETE_WINDOW", lambda: root.destroy())

    main_frame = ttk.Frame(totp_win, padding=20)
    main_frame.pack(expand=True, fill='both')

    info_label = ttk.Label(main_frame, text="Insira o código TOTP:", font=("Helvetica", 10))
    info_label.pack(pady=(0, 5))

    totp_entry = ttk.Entry(main_frame, width=30)
    totp_entry.pack(pady=(0, 10))

    def on_submit():
        totp_code = totp_entry.get()
        totp = pyotp.TOTP(totp_secret)
        
        # O pyotp tem uma janela de tempo de 30 segundos, mas pode ser ajustada
        # Este método verifica se o código é válido dentro de 1 janela de tempo (30s)
        if totp.verify(totp_code): 
            totp_win.destroy()
            iniciar_aplicacao()
        else:
            messagebox.showerror("Erro", "Código TOTP inválido. Verifique se o seu celular está com a hora sincronizada.")
            totp_entry.delete(0, tk.END)

    submit_button = ttk.Button(main_frame, text="Verificar", command=on_submit)
    submit_button.pack(fill='x')
    
    # === Botão de recuperação adicionado ===
    recovery_button = ttk.Button(main_frame, text="Esqueceu o Código TOTP?", command=lambda: [totp_win.destroy(), recover_password_window()])
    recovery_button.pack(pady=(10,0))
    # ==================================

    totp_entry.focus()
    totp_entry.bind("<Return>", lambda event=None: on_submit())

def on_login():
    global login_win, pass_entry, F
    master_password = pass_entry.get()
    if not master_password:
        messagebox.showerror("Erro", "A senha não pode ser vazia.")
        return

    try:
        with open(MASTER_FILE, "rb") as f:
            data = f.read()
            salt = data[:16]
            stored_hash = data[16:48]
            
            # A partir de 80 bytes, a chave TOTP está presente
            totp_secret = None
            if len(data) > 80:
                try:
                    totp_secret_b64 = data[80:]
                    totp_secret = base64.urlsafe_b64decode(totp_secret_b64).decode()
                except (IndexError, binascii.Error, UnicodeDecodeError):
                    totp_secret = None
            
        hashed_password = hashlib.pbkdf2_hmac('sha256', master_password.encode(), salt, 100000)
        
        if hashed_password == stored_hash:
            F = derive_key(master_password, salt)
            
            login_win.destroy()
            if totp_secret:
                # Se o TOTP já está configurado, vai para a tela de validação do código
                totp_window(totp_secret)
            else:
                # Se não, vai para a tela de configuração inicial do TOTP
                setup_totp()
        else:
            messagebox.showerror("Erro", "Senha mestra incorreta.")
    except FileNotFoundError:
        messagebox.showerror("Erro", "Arquivo de segurança não encontrado. O aplicativo precisa ser reiniciado.")
    
def login_window():
    """Cria a janela de login para a senha mestra."""
    global login_win, pass_entry
    login_win = tk.Toplevel(root)
    login_win.title("Desbloquear Password Manager")
    login_win.geometry("350x170")
    login_win.resizable(False, False)
    
    login_win.grab_set()
    login_win.protocol("WM_DELETE_WINDOW", on_closing)

    main_frame_login = ttk.Frame(login_win, padding=20)
    main_frame_login.pack(expand=True, fill='both')

    login_label = ttk.Label(main_frame_login, text="Digite sua Senha Mestra:", font=("Helvetica", 10))
    login_label.pack(pady=(0, 10))

    pass_entry = ttk.Entry(main_frame_login, width=30, show='*')
    pass_entry.pack(pady=(0, 10))

    login_button = ttk.Button(main_frame_login, text="Desbloquear", command=on_login)
    login_button.pack(fill='x')
    
    recovery_button = ttk.Button(main_frame_login, text="Esqueceu a senha?", command=lambda: [login_win.destroy(), recover_password_window()])
    recovery_button.pack(pady=(10,0))
    
    pass_entry.focus()
    pass_entry.bind("<Return>", lambda event=None: on_login())

def show_recovery_key_window(recovery_key):
    """Exibe a chave de recuperação com opção de copiar."""
    key_win = tk.Toplevel(root)
    key_win.title("Chave de Recuperação")
    key_win.geometry("450x250")
    key_win.resizable(False, False)
    key_win.grab_set()

    main_frame = ttk.Frame(key_win, padding=20)
    main_frame.pack(expand=True, fill='both')

    info_label = ttk.Label(
        main_frame, 
        text="Guarde esta chave em um lugar seguro. Ela será sua única forma de recuperação se você perder o acesso ao seu código TOTP.", 
        wraplength=400
    )
    info_label.pack(pady=(0, 10))

    key_entry = tk.Text(main_frame, height=5, width=40)
    key_entry.insert(tk.END, recovery_key)
    key_entry.config(state="disabled") # Torna o texto somente leitura
    key_entry.pack(pady=(0, 10))

    def copy_to_clipboard():
        root.clipboard_clear()
        root.clipboard_append(recovery_key)
        messagebox.showinfo("Copiado", "A chave de recuperação foi copiada para a área de transferência.")

    copy_button = ttk.Button(main_frame, text="Copiar Chave", command=copy_to_clipboard)
    copy_button.pack(fill='x')

    def on_continue():
        key_win.destroy()
        setup_totp()

    continue_button = ttk.Button(main_frame, text="Continuar", command=on_continue)
    continue_button.pack(pady=(10, 0))

# Inicia o processo com a janela de login
if __name__ == "__main__":
    setup_database()
    
    root = ThemedTk(theme="plastik")
    root.withdraw()
    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.after(1000, check_for_inactivity)

    if not os.path.exists(MASTER_FILE):
        create_master_password_window()
    else:
        login_window()
    
    root.mainloop()