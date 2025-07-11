# Instalación y Configuración de PostgreSQL en entorno WSL

Debemos ser capaces de ejecutar consultas de manera local si es que nuestra base de datos en Neón por algun motivo no nos permita migrar o solicitar información.

## Instalacion

- Instalación

```bash
# Instalamos postgresql
sudo apt install postgresql postgresql-contrib -y
```

- Nota:
  - `postgresql`: El motor de base de datos.
  - `postgresql-contrib`: Herramientas adicionales útiles.

---

- Verifica el estado del servicio

```bash
sudo service postgresql status
# Si ves active (running), ¡todo bien!

# Inicia el servicio manualmente
sudo service postgresql start

# Detener servicio
sudo service postgresql stop
```

---

- Configura la contraseña del usuario predeterminado (admin)

```bash
sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD 'tu_contraseña';"

# Si hay errores con los permisos:
sudo -u postgres createuser --superuser --createdb --createrole mi_usuario_pro # Concede permisos explicitamente
sudo -u postgres psql -c "ALTER USER mi_usuario_pro WITH PASSWORD 'contraseña_fuerte';" # Contraseña para tu nuevo usuario

# Reinicia el servicio para aplicar los cambios
sudo service postgresql restart
```

---

- Accede a la linea de comandos de PostgreSQL

```bash
sudo -u postgres psql

# Para salir escribe
\q
```

## Utilidades

- Consulta los privilegios de los usuarios

```bash
# Primero accede a PostgreSQL
\du

# Ejemplo de salida:
List of roles
Role name | Attributes
----------|-----------
mi_user   | NOSUPERUSER, NOCREATEDB, NOCREATEROLE, INHERIT, LOGIN
```

- Concede permisos al usuario

```bash
# Crear bases de datos
ALTER USER mi_user CREATEDB;

# Crear roles
ALTER USER mi_user CREATEROLE;

# Superusuario (tener precaución)
ALTER USER mi_user SUPERUSER;
```

---

- Conceder permisos a una base de datos especifica

```bash
GRANT ALL PRIVILEGES ON DATABASE nombre_db TO mi_user;

# Permisos a una tabla en especifico
GRANT SELECT, INSERT, UPDATE ON TABLE nombre_tabla TO mi_user;
```

- Si un usuario no necesita acceso, retirale los permisos:

```bash
REVOKE ALL PRIVILEGES ON DATABASE nombre_db FROM mi_user;
```
