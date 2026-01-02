from app import application
from app import container

def main() -> None:
    application.application_init()
    db = container.get_database()
    db.generate_tables()

if __name__ == "__main__":
    main()
