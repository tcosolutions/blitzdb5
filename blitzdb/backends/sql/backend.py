from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, scoped_session
from contextlib import contextmanager
import os

class SQLBackend:
    def __init__(self, engine):
        
        # Create the engine and bind the metadata
        self.engine = engine
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

