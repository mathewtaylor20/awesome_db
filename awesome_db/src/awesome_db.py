from db_engine import DBEngine
from db_initializer import DBInitializer

def main(args=None):

    db_initializer = DBInitializer()
    db_initializer.initalize_db()
    db_engine = DBEngine()
    db_engine.run_db()

if __name__ == "__main__":
    main()