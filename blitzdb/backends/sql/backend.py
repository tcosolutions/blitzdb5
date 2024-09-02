from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, scoped_session
from contextlib import contextmanager
import os

class SQLBackend:
    def __init__(self):
        # Define the path to the .checkmate folder in the parent directory
        parent_directory = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
        checkmate_directory = os.path.join(parent_directory, '.checkmate')
        
        # Ensure the .checkmate directory exists
        if not os.path.exists(checkmate_directory):
            os.makedirs(checkmate_directory)

        # Define the database URL
        database_url = f'sqlite:///{os.path.join(checkmate_directory, "db.sqlite")}'
        
        # Create the engine and bind the metadata
        self.engine = create_engine(database_url)
        self.Session = scoped_session(sessionmaker(bind=self.engine))
        self.metadata = MetaData(bind=self.engine)

    def create_tables(self):
        """Create tables in the database."""
        self.metadata.create_all(self.engine)

    def drop_tables(self):
        """Drop all tables in the database."""
        self.metadata.drop_all(self.engine)

    @contextmanager
    def transaction(self):
        """Provide a transactional scope around a series of operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def add(self, obj):
        """Add an object to the session."""
        with self.transaction() as session:
            session.add(obj)

    def delete(self, obj):
        """Delete an object from the session."""
        with self.transaction() as session:
            session.delete(obj)

    def query(self, *args, **kwargs):
        """Query the database."""
        with self.transaction() as session:
            return session.query(*args, **kwargs).all()

    def filter(self, model, *criterion):
        """Filter query results based on criteria."""
        with self.transaction() as session:
            return session.query(model).filter(*criterion).all()

