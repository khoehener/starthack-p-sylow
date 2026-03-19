import mysql.connector
from mysql.connector import Error

def main():
    try:
        conn = mysql.connector.connect(
            host="127.0.0.1",
            port=3306,
            user="root",
            password="p-sylow26",   # hier dein echtes Passwort eintragen
            database="hack2026"
        )

        if conn.is_connected():
            print("✅ Verbindung erfolgreich")

            cursor = conn.cursor()

            cursor.execute("SELECT DATABASE();")
            db_name = cursor.fetchone()[0]
            print(f"Aktive Datenbank: {db_name}")

            cursor.execute("SHOW TABLES;")
            tables = cursor.fetchall()

            print("Tabellen:")
            for table in tables:
                print(f" - {table[0]}")

            cursor.close()
            conn.close()
            print("✅ Test abgeschlossen")

    except Error as e:
        print("❌ Fehler bei der Verbindung:")
        print(e)

if __name__ == "__main__":
    main()